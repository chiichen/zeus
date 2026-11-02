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

## Build

Once the workspace exists, from `svr/`:

```bash
go build -o hello ./apps/hello
```

From the hello app module (Go uses the parent `go.work` automatically):

```bash
cd apps/hello
go build -o hello .
```
