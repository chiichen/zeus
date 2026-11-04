# Consul 配置与进程内路由设计

本文是 `github.com/chiichen/zeus` 分支 `feat-svr` 上的实现计划。后续编码按本文落地。本文不改现有代码。

已有布局保持不动：`svr/packages/` 只放库，`svr/apps/` 只放二进制。`go.work` 与 `go.work.sum` 不进仓库。开发者在 `svr/` 下执行 `go work init && go work use -r .`。CI 使用 Go 1.27.3，并执行 `go test work`、`go build work`、`go vet work`。新模块路径为 `github.com/chiichen/zeus/svr/packages/<name>`。

路由规则是业务规则：环境只有 Prod 与 Dev。泳道、`routeKey` 和按 appid、uid 的回退只在 Dev 生效。这些规则放在框架里，由配置携带。调用链在进程内把一次 RPC 解析成一台下游机器的 `host:port`。RPC 栈是 grpc-go，对外入口是客户端拦截器。

## 1. 目标与非目标

### 目标

- 配置与服务发现都先定义接口，Consul 只是其中一套实现。`route` 与拦截器不 import Consul 客户端。
- 接口分成两组：KV 配置、服务发现（含注册）。
- 在 KV 配置之上实现三类数据：路由（版本钉）、容器变更、灰度发布。Dev 里这些文档按泳道和 `routeKey` 分层；Prod 没有这两段路径。
- 一次调用的终点是一台具体下游地址。拦截器直连这台机器。
- RPC 热路径只读内存快照。Consul 由后台 watch 更新快照。
- Prod 与 Dev 在解析时硬隔离。Dev 内按泳道隔离，泳道之间不回退。同一泳道内，配置按 `routeKey` 回退，回退不改变泳道。
- 选实例有两种策略，`roundRobin` 与 `consistentHash`，由和路由文档一起加载的配置决定，不按环境写死。一致性哈希是通用算法，Prod 和 Dev 都可以选用。
- 单元测试不连接 Consul，CI 现有 `go test work` 能覆盖解析顺序。

### 非目标

- 不实现网关、sidecar、服务网格，也不把 appid、uid、泳道、灰度写进网关路由表。
- 不实现容器编排。本文的「容器变更」是一份配置：哪个版本该接收流量。拉起或销毁容器的系统另行把实例注册进服务发现，并写入变更文档。
- 不新增 `svr/apps/` 二进制。接入发生在未来业务进程的 `main` 里。
- 不做管理端 UI、多数据中心联邦、把 `routeKey` 粘到固定 IP、失败后自动换一台重试。
- 不修改 `svr/packages/greet` 与 `svr/apps/hello`。

## 2. 接口拆分

两组接口分开，是因为它们的数据源、更新频率和失败含义都不同。路由规则存在 KV 里。机器列表来自服务发现。一次解析同时读取两边的内存快照。

Consul 适配器实现这两组接口，是唯一允许 import `github.com/hashicorp/consul/api` 的模块。

### 2.1 KV 配置

模块 `github.com/chiichen/zeus/svr/packages/configkv`。值是原始字节，JSON 结构由 `route` 解析。

```go
package configkv

type Pair struct {
    Key   string
    Value []byte
    Index uint64 // Consul ModifyIndex；内存实现用单调递增整数
}

type KV interface {
    // List 返回 prefix 下的全部键。index 是这份结果的版本，供 watch 去重。
    List(ctx context.Context, prefix string) (pairs []Pair, index uint64, err error)
}

// Watcher 在后台把 prefix 的最新结果推出来。实现必须在失败时保留上一份成功结果，
// 由 route 决定这份结果还能用多久。
type Watcher interface {
    Watch(ctx context.Context, prefix string) (<-chan Update, error)
}

type Update struct {
    Pairs []Pair
    Index uint64
    Err   error // 非空表示本轮刷新失败，Pairs 为空，上一份快照仍然有效
}
```

内存实现是 `configkv.Memory`，测试用它注入键值并手动触发一轮更新。

### 2.2 服务发现

模块 `github.com/chiichen/zeus/svr/packages/discovery`。

```go
package discovery

type Instance struct {
    ID      string
    Service string
    Addr    string // host:port，可直接用于 grpc 拨号
    Env     string // prod 或 dev
    Lane    string // 空视为 "0"；Prod 解析不使用泳道
    Version string
    Healthy bool
}

type Catalog interface {
    // Instances 返回该服务在该环境下通过健康检查的实例，不按泳道过滤。
    Instances(ctx context.Context, service, env string) ([]Instance, error)
}

type Watcher interface {
    Watch(ctx context.Context, service, env string) (<-chan Update, error)
}

type Update struct {
    Instances []Instance
    Index     uint64
    Err       error
}

type Registration struct {
    ID      string
    Service string
    Address string
    Port    int
    Env     string
    Lane    string
    Version string
}

type Registrar interface {
    Register(ctx context.Context, r Registration) error
    Deregister(ctx context.Context, id string) error
}
```

`Catalog` 只按服务名和环境过滤。泳道和版本由解析器过滤。这样 Dev 的 `routeKey` 回退只改配置，不必再查一次 Consul。

内存实现是 `discovery.Memory`。

### 2.3 Consul 适配器实现什么

模块 `github.com/chiichen/zeus/svr/packages/consuladapt`。

| 构造函数 | 实现 | Consul API |
| --- | --- | --- |
| `NewKV(client, prefix)` | `configkv.KV` 与 `configkv.Watcher` | `KV.List`，`QueryOptions.WaitIndex` 阻塞查询 |
| `NewCatalog(client)` | `discovery.Catalog` 与 `discovery.Watcher` | `Health.Service`，`PassingOnly=true`，filter 为 `Meta.env == "<env>"` |
| `NewRegistrar(client)` | `discovery.Registrar` | `Agent.ServiceRegister` / `ServiceDeregister`，附带 gRPC 健康检查 |

约定：

- 地址取 `CONSUL_HTTP_ADDR`，token 取 `CONSUL_HTTP_TOKEN`。适配器接收已经建好的 `*api.Client`，不自己读业务路由配置。
- 阻塞等待用 10 分钟。失败后退避重试：起点 200ms，上限 5s，带抖动。失败更新的 `Err` 非空，不把空列表当成「实例全部下线」。
- RPC 路径不调用这些方法。watch 协程写快照，解析只读快照。
- 查询使用默认一致性（允许 stale），换取 watch 在 Consul 领袖抖动时仍能返回。路由配置不需要线性一致读。
- 注册时写入 Meta：`env`、`lane`、`version`。`env` 取值 `prod` 或 `dev`。`lane` 为空时加载写成 `"0"`。gRPC 检查间隔 10s，超时 2s。进程退出时 `Deregister`。
- 服务发现 watch 的范围是本进程实际要调用的服务列表，由 `route.Router` 在启动时传入。

## 3. 配置模型

前缀固定为 `zeus/`。环境只有两个枚举值：`prod` 与 `dev`。进程环境变量 `ZEUS_ENV` 必填，取值就是这两个字符串。它是这台进程的环境，也是解析时的环境。watch 只订阅这个环境的前缀。

泳道 `lane` 是字符串，默认 `"0"`。`routeKey` 是字符串，默认 `"0"`。二者以及按 appid、uid 的回退只存在于 Dev。Prod 的键路径没有泳道，也没有 `routeKey`。

服务名、泳道、`routeKey`、appid、uid 只允许 `[A-Za-z0-9._:-]`，避免键路径逃出前缀。`routeKey` 用冒号连接两个维度。

### 3.1 键

`confKey` 是文档种类加服务名，两种环境共用同一套 `confKey`。差别只在前缀。

| 环境 | 键 |
| --- | --- |
| Prod | `zeus/prod/{confKey}` |
| Dev | `zeus/dev/{lane}/{routeKey}/{confKey}` |

| confKey | 作用 |
| --- | --- |
| `route/v1/{service}` | 可选版本钉 |
| `pick/v1/{service}` | 选实例策略。两种环境都读它 |
| `gray/v1/{service}/{grayID}` | 灰度计划。同一服务、同一配置层同时只有一份 `status=running` |
| `change/v1/{service}/{changeID}` | 容器变更 |

Prod 示例：`zeus/prod/route/v1/order`。Dev 示例：`zeus/dev/0/42:10001/route/v1/order`。这里泳道是 `"0"`，`routeKey` 是 `42:10001`。

Dev 的配置回退只发生在同一泳道内，按 `routeKey` 从具体到默认。有 appid 或 uid 时，空维度写成 `0`，顺序是：

1. `(appid:uid)`
2. `(appid:0)`
3. `(0:0)`

`(0:0)` 是这套维度的默认配置。相同的候选只保留第一次。例如只有 appid `42`、没有 uid 时，链是 `42:0`，然后 `0:0`。

没有 appid 也没有 uid 时，不走这条链。`routeKey` 用调用方给出的字符串，空则是 `"0"`，只查这一级。

版本钉：

```json
{
  "version": "20261103.1"
}
```

键不存在表示这一层没有版本钉。`version` 为空与键不存在相同。

灰度：

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

`status` 取值 `draft`、`running`、`rolled_back`、`finished`。只有 `running` 参与解析。`percent` 是 0 到 100 的整数。文档不再带泳道字段，泳道已经由 Dev 的路径决定；Prod 没有泳道。

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

`bucket < percent` 进入候选版本。uid 为空时不做百分比切分，留在基线版本，避免同一次用户的重试打到两个版本。

容器变更：

```json
{
  "id": "chg-7",
  "status": "active",
  "version": "20261103.1"
}
```

`status` 取值 `pending`、`active`、`rolled_back`、`done`。只有 `active` 参与解析。`pending` 只记录、不切流量。`rolled_back` 与 `done` 忽略。文档不带泳道和 `routeKey`，这两项由路径决定。

变更与灰度的关系：

- 没有 `running` 灰度，且该配置层恰好一条 `active` 变更：该层全量切到变更里的 `version`。
- 有 `running` 灰度，且恰好一条 `active` 变更：变更的 `version` 必须等于灰度的 `candidate_version`，否则整次解析失败。灰度决定流量比例，变更记录这次滚动的容器版本。
- 有 `running` 灰度但没有 `active` 变更：仍按灰度解析。变更文档可以缺席。
- 同一配置层有两条及以上 `active` 变更：解析失败。
- 部分流量只由灰度的 `percent` 表达。变更文档不带权重。

选实例策略：

```json
{
  "strategy": "roundRobin"
}
```

`strategy` 只接受 `roundRobin` 和 `consistentHash`。文档不存在，或 `strategy` 为空，默认 `roundRobin`。其他字符串是非法配置，按解码失败处理，不安装新快照。Prod 的键是 `zeus/prod/pick/v1/{service}`。Dev 的键是 `zeus/dev/{lane}/{routeKey}/pick/v1/{service}`，和版本钉、灰度、变更一样按 `routeKey` 独立回退。策略不写死在某个环境里。

`consistentHash` 的哈希键：

- Dev：链上的主 `routeKey`（第一项）。配置回退到更低一层时，哈希键不变。
- Prod：路径里没有 `routeKey`。有 appid 或 uid 时，哈希键是 `{appid}:{uid}`，缺的维度写成 `0`。两个都没有时，哈希键是 `"0"`。不把泳道或 `x-zeus-routekey` 编进 Prod 的哈希键，也不按这个字符串回退配置。

Dev 里「配置层」是泳道加上回退停住的那个 `routeKey`。版本钉、选实例策略、灰度集合、变更集合各自独立回退：某一类文档在更具体的 `routeKey` 上不存在时，才用链上的下一层。不要把两层的灰度计划并成一份。

### 3.2 实例

实例不写在 KV 里。注册到 Consul 的服务 Meta：

| Meta | 含义 |
| --- | --- |
| `env` | `prod` 或 `dev` |
| `lane` | 泳道。空加载为 `"0"`。Prod 解析忽略它 |
| `version` | 容器版本，与变更、灰度里的版本字符串相同 |

地址来自注册时的 `Address` 与 `Port`，适配器拼成 `host:port`。

### 3.3 一次调用如何得到地址

下面这次调用的结果是 `10.0.0.3:9090`。

进程 `ZEUS_ENV=dev`，要调用的服务名是 `order`。本次 metadata：appid `42`，uid `10001`，泳道省略，因此泳道是 `"0"`。没有单独的 `routeKey`。

KV：

```text
zeus/dev/0/42:10001/route/v1/order   不存在
zeus/dev/0/42:0/route/v1/order       不存在
zeus/dev/0/0:0/route/v1/order        = {"version":"20261103.1"}
zeus/dev/0/0:0/pick/v1/order         = {"strategy":"consistentHash"}
```

健康实例：

| 地址 | env | lane | version |
| --- | --- | --- | --- |
| 10.0.0.2:9090 | dev | 0 | 20261101.3 |
| 10.0.0.3:9090 | dev | 0 | 20261103.1 |
| 10.0.0.4:9090 | dev | checkout | 20261103.1 |
| 10.1.0.9:9090 | prod | 0 | 20261103.1 |

解析：环境锁定为 `dev`，生产实例不参与。泳道锁定为 `"0"`，`checkout` 不参与，也不回退。`routeKey` 链是 `42:10001`、`42:0`、`0:0`。版本钉在 `0:0` 命中，版本锁定为 `20261103.1`。灰度与变更不再看。选实例策略也落在 `0:0`，是 `consistentHash`。候选只有 `10.0.0.3:9090`。哈希键是主 `routeKey` `42:10001`，不是回退命中的 `0:0`。

同一进程上，appid `42`、uid `10002`、没有版本钉、没有灰度、没有 active 变更，且策略仍是 `consistentHash` 时，候选是泳道 `"0"` 上的全部健康实例。`42:10001` 与 `42:10002` 的哈希不同，可以落到不同地址；各自重复调用时，只要实例集合不变，就仍落在自己上次的那一台。若这一层没有 `pick` 文档，策略是默认的 `roundRobin`。

Prod 进程不读 `zeus/dev/`。它只读 `zeus/prod/{confKey}`，不按泳道和 `routeKey` 过滤。`zeus/prod/pick/v1/order` 决定策略。配成 `consistentHash` 时，哈希键是 `42:10001` 这种 `{appid}:{uid}`，两个都没有则是 `"0"`。配成 `roundRobin` 或缺这份文档时用轮询。

## 4. 解析顺序、优先级与失败策略

### 4.1 顺序

`Resolve` 在入口处各取一次 KV 快照指针和实例快照指针，整个函数不再重读。两个快照不是同一个 Consul 事务生成的，允许相差一轮 watch。

环境隔离先于任何业务规则执行，它不是一条可以回退的路由。

1. **快照可用性。** 任一快照为空，或两者中较旧的 `LoadedAt` 超过 `maxStale`：返回 `Unavailable`。`LoadedAt` 是最近一次成功刷新的本地时间。
2. **环境。** 使用进程的 `ZEUS_ENV`，只接受 `prod` 或 `dev`。metadata 里的 env 若非空且不一致：返回 `FailedPrecondition`。之后的键和实例都带这个 env。
3. **Prod。** 忽略泳道和 `routeKey` 的路径含义。配置只来自 `zeus/prod/{confKey}`。appid 与 uid 不参与选键，只参与灰度分桶，以及 `consistentHash` 的哈希键。然后做第 6 步到第 9 步的版本选择，实例只保留 `env=prod`，不按泳道过滤。第 10 步按 `pick` 文档选策略。
4. **Dev 泳道。** metadata 为空则为 `"0"`。泳道一旦选定就锁住。该泳道没有实例时不改去别的泳道，包括不回退到 `"0"`。
5. **Dev 的 routeKey 链。**
   - 调用方给了非空 `routeKey`，且没有 appid、uid：链只有这一个字符串。
   - 没有 appid、uid，也没有 `routeKey`：链只有 `"0"`。
   - 有 appid 或 uid：忽略默认的单独 `"0"`，链是 `(appid:uid)`、`(appid:0)`、`(0:0)`。缺的维度写成 `0`，去掉重复项。显式 `routeKey` 与 appid、uid 同时出现时，显式值无效，以维度链为准。
6. **版本钉。** 沿 `routeKey` 链找 `route/v1/{service}`，用第一份存在且 `version` 非空的文档。只在当前泳道（Prod 则是全部 prod 实例）加该版本里选实例。没有健康实例则 `Unavailable`。灰度与变更都不再看。
7. **灰度。** 沿链找第一层含有该服务灰度键的 `routeKey`（Prod 只有一层）。该层存在一份 `status=running` 的计划：
   - 多于一份 `running`：`Unavailable`。
   - uid 在 `uid_deny`：基线版本。
   - uid 在 `uid_allow`：候选版本。
   - uid 为空：基线版本。
   - 否则 `bucket(gray.id, uid) < percent` 选候选版本，否则基线版本。
   - 若这一层同时存在一条 `active` 变更，其 `version` 必须等于 `candidate_version`，否则 `Unavailable`。
   - 选中候选版本但没有健康实例：改选基线版本，并计一次回退。基线也没有健康实例：`Unavailable`。
8. **容器变更全量切换。** 没有生效灰度时，沿链找第一层含有该服务变更键的 `routeKey`。该层恰好一条 `active` 变更：全量选它的 `version`。没有健康实例则 `Unavailable`。两条及以上：`Unavailable`。
9. **默认。** 当前 env 上、Dev 再加当前泳道的全部健康实例，不按版本过滤。没有实例则 `Unavailable`。
10. **选一台。** 先沿与版本钉相同的规则读取 `pick/v1/{service}`。Prod 只有 `zeus/prod/pick/v1/{service}`。Dev 在当前泳道的 `routeKey` 链上取第一份存在的文档。没有文档或 `strategy` 为空时用 `roundRobin`。两种环境使用同一套策略，不因为环境改算法。
    - `roundRobin`：匹配实例按 `Addr`、`ID` 排序。对键 `service|env|lane|version|reason` 做原子自增，取模得到下标。Prod 的 `lane` 这一段固定写 `-`。
    - `consistentHash`：实例按 `Addr`、`ID` 排序后，每个实例放 100 个虚节点，虚节点键是 `ID + "#" + strconv.Itoa(i)`。虚节点和哈希键都用 FNV-1a 64 位。哈希键落在环上顺时针的下一个虚节点，该虚节点所属实例中选。集合先排序再建环，结果与进程无关。Dev 的哈希键是主 `routeKey`。Prod 的哈希键是 `{appid}:{uid}`，缺的维度写成 `0`；appid 和 uid 都没有时用 `"0"`。
    - 同一轮解析只选一台。

第 6 步到第 8 步里，一旦版本被版本钉或全量变更钉死，就不再改版本。灰度是唯一允许「候选为空则回基线」的步骤。Dev 的 `routeKey` 回退只选择哪一层配置，不改变已经锁住的泳道，也不改变哈希用的主 `routeKey`。Prod 没有这条回退。

### 4.2 未命中

| 情况 | 结果 | 原因 |
| --- | --- | --- |
| Dev 上某一类文档在更具体的 `routeKey` 不存在 | 同一泳道内继续下一层 | 回退是正常情况，`(0:0)` 是维度默认配置 |
| 维度链和单独的 `"0"` 都没有版本钉、灰度、变更 | 用当前泳道的全部健康实例 | 默认集合 |
| 版本钉或全量变更指定了版本，但没有健康实例 | `Unavailable` | 不改泳道，也不放宽版本 |
| Dev 的非 `"0"` 泳道没有实例 | `Unavailable` | 掉进 `"0"` 会串泳道 |
| 当前范围没有实例 | `Unavailable` | 没有可调用的机器 |
| 灰度候选版本没有实例 | 回退基线版本并计数 | 灰度是分流，不是隔离边界 |
| 灰度基线也没有实例 | `Unavailable` | 没有剩余目标 |
| 多条 `running` 灰度，或多条 `active` 变更，或灰度候选版本与变更版本不一致 | `Unavailable` | 配置有二义，不替操作者挑选 |
| metadata env 与进程 env 不同 | `FailedPrecondition` | 隔离被破坏 |
| Prod 请求带了泳道或 `routeKey` | 不把它们写入键路径，只读 `zeus/prod/` | Prod 没有这两段路径。`consistentHash` 仍用 appid 与 uid 组成哈希键 |
| `pick` 文档缺失或 `strategy` 为空 | `roundRobin` | 默认策略 |
| `strategy` 不是 `roundRobin` 或 `consistentHash` | 拒绝安装该快照 | 非法枚举 |

### 4.3 Consul 不可用

建议策略是：**短时用上一份快照，超过 `maxStale` 则失败关闭。** 默认 `maxStale` 为 30s，可配置。

- watch 出错时保留上一份快照并重试，不把实例列表清掉。
- 从未成功加载过：`Resolve` 返回 `Unavailable`。进程启动等待首次 KV 加载和每个下游服务的首次实例加载，超时 5s 则启动失败。`prod` 与 `dev` 同样处理。
- RPC 期间不访问 Consul，因此 Consul 变慢不会拉长已经有快照的调用。

失败关闭的原因：超过 30s 仍用旧实例列表，会继续拨已经销毁的容器。不存在一个「放开之后仍然保证 Prod 与 Dev、泳道、`routeKey` 隔离」的默认地址。30s 大约覆盖 Consul 健康检查一轮（10s）加上两次 watch 重试，用来渡过短暂故障。

解码失败同样失败关闭到上一份快照：被 watch 的前缀下任意一个键 JSON 不合法、字段未知或字符集不合法，就拒绝安装新快照。首次加载遇到这种键，启动失败。空前缀是合法配置，表示全部走默认实例。解码使用 `DisallowUnknownFields`。

### 4.4 Prod 与 Dev 隔离

隔离由四层同时成立：

1. 进程启动时 `ZEUS_ENV` 不是 `prod` 或 `dev` 就退出。
2. 键前缀按环境分开：`zeus/prod/` 与 `zeus/dev/`。加载器丢弃另一个前缀。
3. 服务发现查询带 `Meta.env` 过滤。
4. 调用 metadata 里的 env 与进程不一致时，当次 RPC 直接失败。

部署上建议两个环境使用不同的 Consul ACL token：Prod 的 token 读不到 `zeus/dev/`，Dev 的 token 读不到 `zeus/prod/`。这是密钥边界。库本身仍按上面四层拒绝串环境。

泳道、`routeKey` 路径和 appid/uid 的配置回退只在 Dev 生效。Prod 的路径是 `zeus/prod/{confKey}`。选实例策略两边都从 `pick/v1/{service}` 读取。

## 5. grpc-go 客户端拦截器

模块 `github.com/chiichen/zeus/svr/packages/grpcroute`。业务代码只跟这个模块和 `route` 打交道。

### 5.1 放在哪里

拦截器运行在**调用方进程**里，包住该进程发出的 gRPC 客户端调用。每个下游服务建一个占位 `*grpc.ClientConn`，服务名在构造拦截器时传入。发现名用这个参数，不从 protobuf 方法名推断。

```go
func UnaryInterceptor(r *route.Router, service string, pool *Pool) grpc.UnaryClientInterceptor
func StreamInterceptor(r *route.Router, service string, pool *Pool) grpc.StreamClientInterceptor
```

占位连接使用 `passthrough:///zeus-router`，只作为生成代码的 `ClientConn`。拦截器不调用这条连接上的 `invoker`。解析完成后，从连接池取出目标地址自己的 `*grpc.ClientConn`，一元调用走 `conn.Invoke`，流式调用走 `conn.NewStream`。

这样做是因为 grpc-go 的 resolver 作用域是一条连接，而 appid、uid、泳道和 `routeKey` 是单次 RPC 的输入。连接级 resolver 不能按请求改目标。`Pick` 能读到 context，但要求所有候选连接预先建好；实例地址会随容器变更进出，按地址懒连接更直接。拦截器在选中地址之后直连，规则留在进程内。

占位连接上只挂路由拦截器。追踪、指标挂在池化连接上，否则 `Invoke` 会跳过占位连接上的后续拦截器。路由拦截器注册为最外层。

池的键是 `host:port`。拨号用 `passthrough:///<host:port>`，单地址，不再做客户端负载均衡。同一个地址并发建连要 singleflight。凭证与占位连接使用调用方传入的同一套 `grpc.DialOption`。

### 5.2 读取的 metadata

键全小写：

| metadata | 常量 | 含义 |
| --- | --- | --- |
| `x-zeus-appid` | `MetaAppID` | 租户。空在维度链里写成 `0` |
| `x-zeus-uid` | `MetaUID` | 用户。空在维度链里写成 `0` |
| `x-zeus-routekey` | `MetaRouteKey` | 显式 `routeKey`。仅在没有 appid、uid 时使用，空为 `"0"` |
| `x-zeus-env` | `MetaEnv` | 必须与进程 `ZEUS_ENV` 一致，或省略。取值 `prod` 或 `dev` |
| `x-zeus-lane` | `MetaLane` | 泳道。仅 Dev 使用，空为 `"0"`。Prod 忽略 |

读取顺序：

1. 本进程放进 context 的 `route.CallMeta`（调用方直接发起时使用）。
2. 入站 metadata（本进程是中游，请求从上游进来）。
3. 出站 metadata。

同一次调用里，context 里的值优先于 metadata。

拦截器把 **appid、uid、env、已经解析出的 lane、主 routeKey** 写进出站 metadata。不写选中的 IP，也不写版本，也不写回退命中的那一层 `routeKey`。下游用同一套身份再算自己的哈希键：Dev 用主 `routeKey`，Prod 用 `{appid}:{uid}` 或 `"0"`。泳道已经由上游定下来，下游即使慢一轮配置快照，也不会把人送进别的泳道。下游自己的 `pick` 文档决定它用轮询还是一致性哈希。

中游进程用一个服务端拦截器 `ForwardCallMeta` 把入站这些键放进 context。这也是进程内代码。每一跳各自解析自己的下游机器。

### 5.3 如何选定目标

```text
CallMeta = 按 5.2 读取
Decision = Router.Resolve(ctx, service, CallMeta)
conn     = Pool.Get(Decision.Addr)
return conn.Invoke 或 conn.NewStream，沿用原来的 ctx、方法和超时
```

`Decision` 包含 `Addr`、`Env`、`Lane`、`RouteKey`、`HashKey`、`Version`、`Reason`、`Pick`。`RouteKey` 只在 Dev 有值，是配置链上的主 `routeKey`；Prod 为空。`HashKey` 是 `consistentHash` 的输入：Dev 等于主 `routeKey`，Prod 是 `{appid}:{uid}` 或 `"0"`。`Reason` 取 `route`、`gray`、`change`、`default` 之一，表示版本从哪一步来。`Pick` 取配置里的 `roundRobin` 或 `consistentHash`，缺省为 `roundRobin`。

错误码：

| 条件 | gRPC code |
| --- | --- |
| 环境不一致 | `FailedPrecondition` |
| 没有快照、快照过期、没有实例、配置二义 | `Unavailable` |

v1 选中一台之后不再换机器。这一台拨号或 RPC 失败，错误返回给调用方。下一次调用按同一规则再选。策略是 `consistentHash` 且实例集合没变时，同一个 `HashKey` 仍是这一台。

### 5.4 与网关的边界

目标地址只存在于调用方进程的连接池里。跨进程传递的是 appid、uid、显式 `routeKey`、环境和泳道这些业务字段。下游框架用本地快照再选自己的下游机器。规则的存储是 Consul KV，执行点是客户端拦截器。

## 6. 包布局

全部是库，`go.mod` 的语言版本写 `go 1.27`，与 CI 工具链一致。模块里没有 `package main`。不提交 `go.work`。CI 已执行 `go work use -r .`，新模块放在 `svr/packages/` 下会被收进去，不必改工作流。

```text
svr/packages/configkv/          github.com/chiichen/zeus/svr/packages/configkv
  go.mod
  kv.go                         KV、Watcher、Pair
  memory.go                     内存实现
  memory_test.go

svr/packages/discovery/         github.com/chiichen/zeus/svr/packages/discovery
  go.mod
  catalog.go                    Catalog、Watcher、Registrar、Instance
  memory.go
  memory_test.go

svr/packages/consuladapt/       github.com/chiichen/zeus/svr/packages/consuladapt
  go.mod                        依赖 configkv、discovery、hashicorp/consul/api
  kv.go                         KV 的 Consul 实现
  catalog.go                    Catalog 的 Consul 实现
  registrar.go                  Registrar 的 Consul 实现
  watch.go                      阻塞查询与退避
  adapt_test.go                 httptest 模拟 KV 与 Health 的 HTTP 响应

svr/packages/route/             github.com/chiichen/zeus/svr/packages/route
  go.mod                        只依赖 configkv、discovery
  meta.go                       CallMeta、metadata 键、字符集
  model.go                      Route、Pick、Gray、Change 的 JSON
  keys.go                       prod 与 dev 的键路径、routeKey 链
  snapshot.go                   快照构建、原子替换、maxStale
  resolve.go                    第 4 节的顺序
  hash.go                       两种环境共用的一致性哈希环
  resolve_test.go
  router.go                     启动 watch、等待首次加载

svr/packages/grpcroute/         github.com/chiichen/zeus/svr/packages/grpcroute
  go.mod                        依赖 route、google.golang.org/grpc
  interceptor.go                一元与流式客户端拦截器、ForwardCallMeta
  pool.go                       按 host:port 的连接池
  interceptor_test.go           bufconn，两台后端，断言 routeKey 哈希选中哪一台
```

依赖方向：

```text
grpcroute → route → configkv
                  → discovery
consuladapt → configkv
            → discovery
            → hashicorp/consul/api
```

`route` 不依赖 `consuladapt`，也不依赖 grpc。测试用内存实现就能覆盖第 4 节。

不新增 app。未来业务 `main` 自己组装：

```go
client, err := api.NewClient(api.DefaultConfig()) // 地址和 token 来自环境变量
kv := consuladapt.NewKV(client, "zeus/")
cat := consuladapt.NewCatalog(client)
rt, err := route.Start(ctx, kv, cat, route.Options{
    Env:      os.Getenv("ZEUS_ENV"), // prod 或 dev
    Services: []string{"order"},
    MaxStale: 30 * time.Second,
})
pool := grpcroute.NewPool(dialOpts...)
cc, err := grpc.NewClient(
    "passthrough:///zeus-router",
    grpc.WithChainUnaryInterceptor(grpcroute.UnaryInterceptor(rt, "order", pool)),
    grpc.WithChainStreamInterceptor(grpcroute.StreamInterceptor(rt, "order", pool)),
)
```

本服务要被别人发现时，同一个 `main` 调用 `consuladapt.NewRegistrar`。hello 应用不接入。

## 7. 分阶段实现

每一阶段结束时，从 `svr/` 跑通 `go test work`、`go build work`、`go vet work`。测试不拨真实 Consul。

### 阶段 1：接口与内存实现

- 建立 `configkv`、`discovery` 两个模块。
- 落地第 2 节的接口和内存实现。
- 内存 watch 可以由测试手动推送 `Update`。

本阶段没有解析逻辑。

### 阶段 2：模型与解析

- 建立 `route`。
- 实现键解析、JSON 解码、快照、第 4 节的全部顺序，以及两种选实例策略。
- 表驱动测试至少覆盖：Prod 只读 `zeus/prod/` 且键路径不含泳道和 `routeKey`；Dev 泳道默认 `"0"` 且不回退到其他泳道；`(appid:uid)` 命中后不再看更低层；缺文档时 `(appid:uid)` 落到 `(appid:0)` 再落到 `(0:0)`；没有 appid、uid 时只用 `"0"`；版本钉缺实例；灰度 allow/deny/percent；uid 为空留在基线；候选为空回基线；基线也为空；变更全量切换；变更与灰度版本不一致；多条 active 变更；环境不一致；快照缺失；超过 `maxStale`；`pick` 缺失时两种环境都是 `roundRobin`；Dev 的 `consistentHash` 用主 `routeKey` 而不是回退层；Prod 的 `consistentHash` 用 `{appid}:{uid}`，都没有时用 `"0"`；同一 `HashKey` 在集合不变时两次解析得到同一地址。
- 用可注入的时钟测 `maxStale`。
- 观察口先留一个空实现：

```go
type Observer interface {
    GrayFallback(service, env, lane string)
    Stale(service string)
}
```

不引入 Prometheus。

### 阶段 3：Consul 适配器

- 建立 `consuladapt`。
- KV 与 Health 的阻塞查询、索引回绕（`LastIndex` 变小则从 0 再 watch）、失败退避、注册与反注册。
- 用 `httptest` 返回 Consul HTTP API 的列表与健康检查 JSON，断言 Meta 过滤、`PassingOnly` 和「出错时不发送空实例列表」。
- `route.Start` 接上 watcher：首次成功才返回；解码失败不替换已有快照。Prod 只 watch `zeus/prod/`，Dev 只 watch `zeus/dev/`。

### 阶段 4：拦截器

- 建立 `grpcroute`。
- 一元与流式拦截器、连接池、metadata 读取与转发、`ForwardCallMeta`。
- `bufconn` 起两个后端。`consistentHash` 指向其中一个地址时，断言只有该后端收到请求。Dev 用例检查出站 metadata 里的 lane 与主 `routeKey`。Prod 用例检查哈希键是 `{appid}:{uid}`，并且没有按泳道过滤。
- 环境不一致时断言 `FailedPrecondition`，且两个后端都没有收到请求。

### 明确推迟

- 管理端、探测二进制、Prometheus 导出。
- 同一 RPC 失败后改拨另一台。
- 把哈希键写成一条存库的 IP 绑定。`consistentHash` 只对当前健康集合建环；实例进出时，只有环上被切开的那段哈希键会换机器。
- 多数据中心、Consul prepared query、把 ACL 自动配进库里。
- 写变更文档的发布系统，以及拉起容器的控制器。
- 服务端业务拦截器之外的协议字段。不新增 proto；路由字段只走 metadata。
- `hello` 不作为示例接入。

阶段 4 完成后，调用方从 KV 和服务发现的内存快照得到一台下游机器，并经拦截器直连。这就是本文的完成线。

## 8. 风险

### 一致性

KV 与服务目录不是一个事务。灰度文档可以先于新版本实例出现，实例也可以先于文档注册。解析把入口处的两份快照用到底，单次调用内部不撕裂。两份快照彼此可以相差一轮 watch。

操作顺序：先让新版本实例健康，再把灰度 `percent` 从 0 调高，或再把变更标为 `active`。库不强制这个顺序。候选版本还没有实例时，灰度会回退基线并计数；全量变更找不到实例时，调用失败。后者用来避免在没有新容器时把全部流量切走。

### 热加载

快照不可变，用原子指针替换。更新协程从本环境前缀全量重建，不做单键补丁。一个坏键会让新快照整体不生效，上一份快照继续服务，直到有人改掉这个键。代价是一次笔误会挡住同前缀下的其他合法变更。这个代价换来的是：坏键不会被当成「规则已删除」。

正在执行的 RPC 用它入口时的快照。新快照只影响下一次 `Resolve`。

### 实例列表过期

健康检查通过，不代表进程此刻还活着。检查间隔 10s，加上 `maxStale` 30s，最坏情况下会在大约这个量级内继续选中已死地址。v1 不在失败后改拨。策略是 `consistentHash` 时，同一个 `HashKey` 会继续落到这台已死地址，直到它从健康集合消失；消失之后环重算，这个键落到新的后继。`roundRobin` 则下次取模时可能选到别的地址。

watch 失败时禁止把空列表装进快照。空列表只在 Consul 明确返回「没有通过检查的实例」时装入。这两件事必须在适配器测试里分开。

连接池会保留到已从目录消失的地址的连接。v1 不主动拆。该地址不再被选中后，连接只是闲置。闲置回收推迟，避免和解析逻辑缠在一起。

### 灰度回滚

回滚方式：把灰度 `status` 写成 `rolled_back`，或把 `percent` 写成 0。下一轮 watch 之后的新调用回到基线版本。已经选中候选地址的在途 RPC 不取消。回滚是「下一次调用」，不是连接排空。

全量变更的回滚：把该变更写成 `rolled_back`。若旧版本实例仍在目录里，默认步骤会重新选到它们。若旧实例已经注销，回滚后没有机器，调用 `Unavailable`。发布系统在回滚完成前应保留旧版本实例。

`uid_allow` 会绕过 `percent=0`。回滚时若要连这些用户也离开候选版本，要同时清空 `uid_allow`，或把状态改为 `rolled_back`。解析器在 `rolled_back` 时不读 allow 列表。

### 配置二义

多份 `running` 灰度或多份 `active` 变更会让该服务在这一配置层的调用失败。这是有意的。自动挑一份会把回滚目标选错。

Dev 的回退按文档种类整层接管。更具体的 `routeKey` 上只要有一份该服务的灰度键，就不再读默认层的灰度。只写了一半的租户配置会挡住 `(0:0)` 上的计划。操作时要么写全这一层，要么不要在这一层放这类键。

Prod 不读 Dev 的泳道和 `routeKey`。带到 Prod 的泳道头被忽略，请求仍落在 `zeus/prod/` 和 `env=prod` 的实例上，而不是打进 Dev。环境不一致则直接失败。这两种行为不同，排障时要分开看。
