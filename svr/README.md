# svr

Go workspace for server modules.

## Layout

- `packages/` holds library modules only. They must not contain `package main` or a `main` function.
- `apps/` holds application modules. Each one builds a runnable binary (`package main`).

The modules are committed. The workspace file is local. `go work init` takes module directories and does not expand `*` or `./...`. After clone, create the workspace from `svr/` and add every module under it:

```bash
cd svr
go work init
go work use -r .
```

`go.work.sum` may appear locally after `go work sync` or a build. Leave `go.work` and `go.work.sum` untracked.

## Check

From `svr/`, `./...` does not match the workspace modules. That directory is not inside a module, and neither is `./apps` or `./packages`. An import path can: `github.com/chiichen/zeus/svr/...` matches every package under that prefix. Go 1.25 and later also accept `work`, which means every package in the workspace:

```bash
go test work
go build work
go vet work
```

`go build work` only checks that the packages compile. To write the hello binary:

```bash
go build -o hello ./apps/hello
```

From the hello app module (Go uses the parent `go.work` automatically):

```bash
cd apps/hello
go build -o hello .
```
