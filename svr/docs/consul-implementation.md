# Consul 实现

本文描述配置与路由规则如何落到进程里。键的内容和解析顺序见 [config-routing.md](config-routing.md)。这里才出现 Consul 的 API、Meta 和阻塞查询。

`route` 与拦截器不 import Consul 客户端。Consul 适配器是唯一允许 import `github.com/hashicorp/consul/api` 的模块。RPC 热路径只读内存快照，watch 协程负责更新快照。

## 1. 模块

全部是库，`go.mod` 写 `go 1.27`。没有 `package main`。不提交 `go.work`。

```text
svr/packages/configkv/       KV、Watcher、内存实现
svr/packages/discovery/      实例目录、注册、内存实现
svr/packages/consuladapt/    上面两组接口的 Consul 实现
svr/packages/route/          键路径、JSON、快照、解析、一致性哈希
svr/packages/grpcroute/      进程内客户端拦截器与连接池
```

依赖：

```text
grpcroute → route → configkv
                  → discovery
consuladapt → configkv
            → discovery
            → hashicorp/consul/api
```

`route` 不依赖 `consuladapt`，也不依赖 grpc。解析顺序用内存实现就能测完。

### 1.1 KV

模块 `github.com/chiichen/zeus/svr/packages/configkv`。值是原始字节，JSON 由 `route` 解码，未知字段拒绝。

```go
package configkv

type Pair struct {
    Key   string
    Value []byte
    Index uint64 // Consul 实现里是 ModifyIndex；内存实现用单调递增整数
}

type KV interface {
    // List 返回 prefix 下的全部键。index 是这份结果的版本，供 watch 去重。
    List(ctx context.Context, prefix string) (pairs []Pair, index uint64, err error)
}

// Watcher 在后台把 prefix 的最新结果推出来。失败时保留上一份成功结果。
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

### 1.2 服务发现

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

内存实现是 `discovery.Memory`。`Catalog` 只按服务名和环境过滤，这样 Dev 的 `routeKey` 回退只改配置，不必再查一次目录。

### 1.3 文件

```text
svr/packages/configkv/     kv.go、memory.go
svr/packages/discovery/    catalog.go、memory.go
svr/packages/consuladapt/  kv.go、catalog.go、registrar.go、watch.go
svr/packages/route/        meta.go、model.go、keys.go、snapshot.go、resolve.go、hash.go、router.go
svr/packages/grpcroute/    interceptor.go、pool.go
```

`consuladapt` 的测试用 `httptest` 返回 Consul HTTP JSON。`grpcroute` 的测试用 `bufconn`。不新增 `svr/apps/` 二进制，不改 `greet` 和 `hello`。

实例字段与 Consul 服务 Meta 的对应：

| 解析器字段 | Consul |
| --- | --- |
| `env` | Meta `env`，`prod` 或 `dev` |
| `lane` | Meta `lane`。空加载为 `"0"`。Prod 解析不使用 |
| `version` | Meta `version`，与版本钉、灰度、变更里的版本字符串相同 |
| `addr` | 注册时的 `Address` 与 `Port`，拼成 `host:port` |
| `id` | 服务实例 ID |

目录查询只按服务名和 `Meta.env` 过滤，不按泳道过滤。泳道和版本由 `route` 在快照上过滤。

## 2. Consul 适配

| 构造 | 实现 | Consul API |
| --- | --- | --- |
| `NewKV(client, prefix)` | KV 列表与 watch | `KV.List`，`QueryOptions.WaitIndex` 阻塞查询 |
| `NewCatalog(client)` | 实例列表与 watch | `Health.Service`，`PassingOnly=true`，filter 为 `Meta.env == "<env>"` |
| `NewRegistrar(client)` | 注册与反注册 | `Agent.ServiceRegister` / `ServiceDeregister`，附带 gRPC 健康检查 |

约定：

- 地址取 `CONSUL_HTTP_ADDR`，token 取 `CONSUL_HTTP_TOKEN`。适配器接收已经建好的 `*api.Client`，不自己读路由文档。
- 阻塞等待 10 分钟。失败后退避：起点 200ms，上限 5s，带抖动。失败更新的 `Err` 非空，不把空列表当成实例全部下线。
- 查询使用默认一致性，允许 stale。路由配置不要求线性一致读。
- 注册写入 Meta：`env`、`lane`、`version`。gRPC 检查间隔 10s，超时 2s。进程退出时 `Deregister`。
- watch 范围是本进程要调用的服务，由 `route.Router` 在启动时传入。Prod 只 watch `zeus/prod/`，Dev 只 watch `zeus/dev/`。
- `LastIndex` 变小则从 0 再 watch。

空列表只在 Consul 明确返回「没有通过检查的实例」时写入快照。watch 出错时保留上一份快照。这两件事要在适配器测试里分开。

建议 Prod 与 Dev 使用不同的 ACL token，使一边的 token 读不到另一边的前缀。库本身仍然按环境前缀和 `Meta.env` 拒绝串环境。

## 3. 快照

`route.Start` 接上 watcher。首次 KV 加载和每个下游服务的首次实例加载成功才返回，超时 5s 则启动失败。`prod` 与 `dev` 相同。

更新协程按本环境前缀全量重建快照，不做单键补丁。一个非法键拒绝整份新快照，上一份继续服务。快照不可变，用原子指针替换。正在执行的调用用它入口时的快照。

两份快照不是同一个 Consul 事务生成的，允许相差一轮 watch。先让新版本实例健康，再提高灰度 `percent`，或再把变更标为 `active`。库不强制这个顺序。

默认 `maxStale` 为 30s，大约覆盖一轮 10s 健康检查加上两次 watch 重试。超过这个时间仍用旧实例列表，会继续拨已经销毁的容器。RPC 期间不访问 Consul。

## 4. 客户端拦截器

模块 `github.com/chiichen/zeus/svr/packages/grpcroute`。

```go
func UnaryInterceptor(r *route.Router, service string, pool *Pool) grpc.UnaryClientInterceptor
func StreamInterceptor(r *route.Router, service string, pool *Pool) grpc.StreamClientInterceptor
```

拦截器在调用方进程里，包住该进程发出的 gRPC 调用。每个下游服务一个占位 `*grpc.ClientConn`，服务名由构造参数给出，不从方法名推断。

占位连接使用 `passthrough:///zeus-router`。拦截器不走这条连接的 `invoker`。解析完成后，从按 `host:port` 分桶的连接池取出目标连接，一元调用走 `conn.Invoke`，流式调用走 `conn.NewStream`。同一地址并发建连要 singleflight。池化连接使用调用方传入的同一套 `DialOption`。

连接级 resolver 不能按单次请求的 appid、uid、泳道改目标，所以规则留在拦截器里，而不是 grpc-go 的负载均衡器里。占位连接上只挂路由拦截器，追踪和指标挂在池化连接上。

metadata 全小写：`x-zeus-appid`、`x-zeus-uid`、`x-zeus-routekey`、`x-zeus-env`、`x-zeus-lane`。读取顺序是 context 里的 `CallMeta`、入站 metadata、出站 metadata。context 优先。

出站写回 appid、uid、env、解析后的 lane、主 `routeKey`。不写 IP，不写版本，不写回退命中的那一层 `routeKey`。下游用自己的 `pick` 文档决定轮询还是一致性哈希；Dev 的哈希键用主 `routeKey`，Prod 用 `{appid}:{uid}` 或 `"0"`。

中游用服务端拦截器 `ForwardCallMeta` 把入站字段放进 context。每一跳各自解析自己的下游。没有网关。选中的地址只存在于调用方进程的连接池里。

环境不一致返回 `FailedPrecondition`。没有快照、快照过期、没有实例、配置二义返回 `Unavailable`。v1 选中一台之后不换机器。`consistentHash` 且实例集合不变时，同一个哈希键仍是这一台。

连接池会留下已经从目录消失的地址。v1 不主动拆，避免和解析缠在一起。

业务进程自己组装，不新增 app，也不改 `hello`：

```go
client, err := api.NewClient(api.DefaultConfig())
kv := consuladapt.NewKV(client, "zeus/")
cat := consuladapt.NewCatalog(client)
rt, err := route.Start(ctx, kv, cat, route.Options{
    Env:      os.Getenv("ZEUS_ENV"),
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

本服务要被别人发现时，同一个 `main` 调用 `consuladapt.NewRegistrar`。

## 5. 实现顺序

每一阶段从 `svr/` 跑通 `go test work`、`go build work`、`go vet work`。测试不拨真实 Consul。

1. `configkv` 与 `discovery` 的接口和内存实现。内存 watch 由测试推送更新。
2. `route`：键路径、四种 JSON、快照、解析顺序、两种选实例策略。表驱动测试覆盖 Prod 只读 `zeus/prod/`、Dev 泳道不回退、`(appid:uid)` 到 `(appid:0)` 再到 `(0:0)`、`pick` 缺省为 `roundRobin`、Dev 哈希用主 `routeKey`、Prod 哈希用 `{appid}:{uid}` 或 `"0"`、灰度与变更的失败分支、`maxStale`。
3. `consuladapt`：阻塞查询、索引回绕、退避、注册与反注册。`httptest` 返回 Consul HTTP JSON，断言 Meta 过滤、`PassingOnly`，以及出错时不发送空实例列表。
4. `grpcroute`：一元与流式拦截器、连接池、metadata 转发。`bufconn` 两个后端。`consistentHash` 只打中一台。环境不一致时两个后端都没有请求。

推迟：管理端、Prometheus、失败后改拨另一台、把哈希键存成固定 IP、多数据中心、prepared query、在库里自动配置 ACL、发布系统和容器控制器、新增 proto。
