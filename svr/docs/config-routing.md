# KV 配置与路由规则

本文只描述配置键、每类键的内容，以及这些配置如何决定一次调用的版本和下游地址。不涉及配置存储的实现。实例列表是解析器的另一份输入，这里只使用它的字段：`env`、`lane`、`version`、`addr`、`id`。

环境只有两个值：`prod` 与 `dev`。进程用 `ZEUS_ENV` 标明自己的环境，取值就是这两个字符串。泳道、`routeKey` 以及按 appid、uid 的配置回退只在 Dev 存在。Prod 的键路径没有泳道，也没有 `routeKey`。

## 1. 配置类型

共四种。`confKey` 是文档种类加服务名，两种环境共用。

| 类型 | confKey | 作用 |
| --- | --- | --- |
| 版本钉 | `route/v1/{service}` | 可选。把该服务钉在一个版本上 |
| 选实例策略 | `pick/v1/{service}` | `roundRobin` 或 `consistentHash`。两种环境都读 |
| 灰度 | `gray/v1/{service}/{grayID}` | 按 uid 把流量分到基线版本或候选版本 |
| 容器变更 | `change/v1/{service}/{changeID}` | 记录这次滚动要接收流量的版本。本身不带权重 |

键路径：

| 环境 | 键 |
| --- | --- |
| Prod | `zeus/prod/{confKey}` |
| Dev | `zeus/dev/{lane}/{routeKey}/{confKey}` |

`lane` 和 `routeKey` 都是字符串，默认 `"0"`。服务名、泳道、`routeKey`、appid、uid 只允许 `[A-Za-z0-9._:-]`。

Prod 示例：`zeus/prod/route/v1/order`。Dev 示例：`zeus/dev/0/42:10001/route/v1/order`。

## 2. 各类配置的内容

### 2.1 版本钉 `route/v1/{service}`

```json
{
  "version": "20261103.1"
}
```

键不存在，或 `version` 为空，表示这一层没有版本钉。命中后只在该版本的实例里选择。没有健康实例则失败，不再看灰度和变更。

### 2.2 选实例策略 `pick/v1/{service}`

```json
{
  "strategy": "roundRobin"
}
```

`strategy` 只接受：

| 值 | 行为 |
| --- | --- |
| `roundRobin` | 在排序后的候选实例上轮询 |
| `consistentHash` | 用哈希键在候选实例的环上选一台 |

文档不存在，或 `strategy` 为空，默认 `roundRobin`。其他字符串是非法配置，整份新快照不生效。策略不按环境写死。Prod 读 `zeus/prod/pick/v1/{service}`。Dev 读 `zeus/dev/{lane}/{routeKey}/pick/v1/{service}`，并按下面的 `routeKey` 链独立回退。

### 2.3 灰度 `gray/v1/{service}/{grayID}`

```json
{
  "id": "g1",
  "status": "running",
  "baseline_version": "20261101.3",
  "candidate_version": "20261103.1",
  "percent": 10,
  "uid_allow": ["10001"],
  "uid_deny": ["10002"]
}
```

`status` 取值 `draft`、`running`、`rolled_back`、`finished`。只有 `running` 参与解析。同一服务、同一配置层同时只能有一份 `running`。`percent` 是 0 到 100 的整数。文档不带泳道和 `routeKey`，这两项由键路径决定。

分桶：

```go
func bucket(grayID, uid string) int {
    h := fnv.New32a()
    _, _ = h.Write([]byte(grayID))
    _, _ = h.Write([]byte{0})
    _, _ = h.Write([]byte(uid))
    return int(h.Sum32() % 100)
}
```

判定顺序：uid 在 `uid_deny` 用基线；在 `uid_allow` 用候选；uid 为空留在基线，避免同一次用户的重试打到两个版本；否则 `bucket < percent` 用候选，不然用基线。`uid_allow` 在 `percent` 为 0 时仍然进入候选。候选版本没有健康实例时改选基线并计数。基线也没有则失败。

`draft`、`rolled_back`、`finished` 不参与解析，也不读 allow 与 deny。回滚若要连 allow 里的用户也离开候选版本，把 `status` 写成 `rolled_back`，或在仍为 `running` 时清空 `uid_allow` 并把 `percent` 写成 0。已经按旧规则选中的在途调用不改目标，回滚只影响下一次解析。

### 2.4 容器变更 `change/v1/{service}/{changeID}`

```json
{
  "id": "chg-7",
  "status": "active",
  "version": "20261103.1"
}
```

`status` 取值 `pending`、`active`、`rolled_back`、`done`。只有 `active` 参与解析。`pending` 只记录、不切流量。`rolled_back` 与 `done` 忽略。文档不带权重，也不带泳道和 `routeKey`。

与灰度的关系，都在同一配置层内判断：

- 没有 `running` 灰度，且恰好一条 `active` 变更：全量切到变更里的 `version`。没有该版本的健康实例则失败。
- 有 `running` 灰度，且恰好一条 `active` 变更：变更的 `version` 必须等于灰度的 `candidate_version`，否则失败。灰度决定比例，变更记录滚动版本。
- 有 `running` 灰度但没有 `active` 变更：仍按灰度解析。
- 两条及以上 `active` 变更：失败。

回滚是把 `status` 写成 `rolled_back`。之后不再按这份变更钉版本，改走默认规则。旧版本实例还在时，默认规则会重新选到它们。旧实例已经不在时，这次调用失败。回滚完成前应保留旧版本实例。

## 3. Dev 的 routeKey 回退

回退只发生在同一泳道内，不改变泳道。有 appid 或 uid 时，空维度写成 `0`，顺序是：

1. `(appid:uid)`
2. `(appid:0)`
3. `(0:0)`

`(0:0)` 是这套维度的默认配置。相同候选只保留第一次。只有 appid `42`、没有 uid 时，链是 `42:0`，然后 `0:0`。

没有 appid 也没有 uid 时，不走这条链。`routeKey` 用调用方给出的字符串，空则是 `"0"`，只查这一级。显式 `routeKey` 与 appid、uid 同时出现时，以维度链为准。

版本钉、选实例策略、灰度集合、变更集合各自独立回退。某一类文档在更具体的 `routeKey` 上不存在，才用下一层。不要把两层的灰度计划并成一份。更具体的一层只要有该服务的一份灰度键，就不再读默认层的灰度，哪怕这一层没有 `running`。变更键同样：这一层有键但没有 `active`，也不再读更低一层的变更。只写了一半的租户配置会挡住 `(0:0)` 上的同类计划。

## 4. 一次调用怎么解析

解析开始时各取一份配置快照和一份实例快照，函数内不再重读。环境隔离先于业务规则。

调用输入，不是 KV 键：

| 输入 | 含义 |
| --- | --- |
| `appid` | 租户。空在维度链里写成 `0` |
| `uid` | 用户。空在维度链里写成 `0` |
| `routeKey` | 显式路由键。仅在没有 appid、uid 时使用 |
| `env` | 必须与进程环境一致，或省略 |
| `lane` | 泳道。仅 Dev 使用，空为 `"0"`。Prod 不写入键路径 |

顺序：

1. 任一快照为空，或较旧的一份超过 `maxStale`（默认 30s）：失败。
2. 环境用进程的 `ZEUS_ENV`。调用里的 env 非空且不一致：失败。
3. Prod：只读 `zeus/prod/{confKey}`。忽略泳道和 `routeKey` 路径。appid 与 uid 不参与选键，只参与灰度分桶和一致性哈希的键。实例只留 `env=prod`，不按泳道过滤。
4. Dev：泳道取调用值，空为 `"0"`，然后锁住。该泳道没有实例时不改去其他泳道，包括不回退到 `"0"`。
5. Dev：按第 3 节建立 `routeKey` 链。
6. 版本钉。沿链取第一份 `version` 非空的 `route/v1/{service}`。Prod 只有一层。钉住后不再看灰度和变更。
7. 灰度。沿链取第一层含有该服务灰度键的位置。规则见 2.3。多于一份 `running` 则失败。
8. 变更全量切换。没有生效灰度时，沿链取第一层含有该服务变更键的位置。规则见 2.4。
9. 默认。当前环境下的健康实例；Dev 再限制为当前泳道。不按版本过滤。没有实例则失败。
10. 选一台。读取 `pick/v1/{service}`，缺省 `roundRobin`。
    - `roundRobin`：候选按 `addr`、`id` 排序后轮询。轮询键是 `service|env|lane|version|reason`。Prod 的 `lane` 段固定为 `-`。
    - `consistentHash`：候选按 `addr`、`id` 排序后，每个实例 100 个虚节点，虚节点键是 `id + "#" + 序号`。虚节点和哈希键都用 FNV-1a 64 位，取顺时针的下一个虚节点。Dev 的哈希键是链上的主 `routeKey`，不是回退命中的那一层。Prod 没有 `routeKey` 路径，哈希键是 `{appid}:{uid}`，缺的维度写成 `0`；两个都没有时用 `"0"`。不把泳道或显式 `routeKey` 编进 Prod 的哈希键。

版本一旦被版本钉或全量变更钉死，就不再改。灰度是唯一允许「候选为空则回基线」的步骤。

结果字段：`addr`、`env`、`lane`、`routeKey`、`hashKey`、`version`、`reason`、`pick`。`routeKey` 只在 Dev 有值。`reason` 取 `route`、`gray`、`change`、`default`。`pick` 取实际策略。

未命中：

| 情况 | 结果 |
| --- | --- |
| Dev 上某类文档在更具体的 `routeKey` 不存在 | 同一泳道内看下一层 |
| 没有版本钉、灰度、变更 | 用当前范围的全部健康实例 |
| 钉死的版本没有健康实例 | 失败，不改泳道 |
| Dev 非 `"0"` 泳道没有实例 | 失败，不掉进 `"0"` |
| 灰度候选为空 | 回基线并计数；基线也为空则失败 |
| 多份 `running` 灰度、多份 `active` 变更，或两者版本不一致 | 失败 |
| 调用 env 与进程 env 不同 | 失败 |
| Prod 带了泳道或 `routeKey` | 不写入键路径。`consistentHash` 仍用 appid 与 uid |
| `pick` 缺失或 `strategy` 为空 | `roundRobin` |
| `strategy` 不是两个合法值 | 整份新快照不生效 |

任一键的内容不合法时，不安装新快照，继续用上一份。第一次加载就遇到非法键则不能开始解析。空前缀是合法配置，表示全部走默认实例。

## 5. 例子

进程环境 `dev`，服务 `order`，appid `42`，uid `10001`，泳道省略，因此泳道是 `"0"`。

```text
zeus/dev/0/42:10001/route/v1/order   不存在
zeus/dev/0/42:0/route/v1/order       不存在
zeus/dev/0/0:0/route/v1/order        = {"version":"20261103.1"}
zeus/dev/0/0:0/pick/v1/order         = {"strategy":"consistentHash"}
```

实例里 `10.0.0.3:9090` 是 `dev`、泳道 `"0"`、版本 `20261103.1`。其他泳道和 `prod` 不参与。版本钉落在 `0:0`，策略也落在 `0:0`。哈希键仍是主 `routeKey` `42:10001`。

同一泳道上，`42:10002` 与 `42:10001` 哈希不同，可以落到不同地址。实例集合不变时，同一个哈希键仍落在同一台。

Prod 只读 `zeus/prod/pick/v1/order` 这类键。配成 `consistentHash` 时，哈希键是 `{appid}:{uid}`，两个都没有则是 `"0"`。
