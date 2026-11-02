# svr

Go workspace for server modules.

## Layout

- `packages/` holds library modules only. They must not contain `package main` or a `main` function.
- `apps/` holds application modules. Each one builds a runnable binary (`package main`).

`go.work` is the workspace root. An app import of another module in this workspace resolves to that local module.

## Build

From the workspace:

```bash
cd svr
go build -o hello ./apps/hello
```

From the hello app module (Go uses the parent `go.work` automatically):

```bash
cd svr/apps/hello
go build -o hello .
```

After changing module requirements:

```bash
cd svr
go work sync
```
