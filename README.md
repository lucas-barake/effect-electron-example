# effect-electron-example

A complete, runnable example of wiring [**Effect RPC**](https://effect.website) (`@effect/rpc`) and
[**effect-atom**](https://github.com/tim-smart/effect-atom) (`@effect-atom/atom`) across the Electron
**main ↔ renderer** boundary over a `MessagePort`, with a small **todos** app whose server (the main
process) persists to the **file system**.

The IPC foundation (transport, contract, atom layer) follows an established production pattern for
`@effect/rpc` over Electron's `MessagePort`, extended here into a self-contained working app with full
test coverage.

## What it shows

- A **single schema-defined RPC contract** (`TodosRpcs`) that is the source of truth for both sides of
  the wire — payloads, successes, and typed errors are validated on the client and the server.
- A custom **`@effect/rpc` transport over a `MessagePort`** (MsgPack serialization), as a renderer-side
  `RpcClient.Protocol` and a main-side `RpcServer.Protocol`, including:
  - **port handoff** — the server rebinds to a fresh port on every renderer load;
  - **stream interruption on swap** — a reloaded renderer's in-flight server streams are interrupted
    (via the server protocol's `disconnects` mailbox).
- A realistic **server**: a `FileSystem`-backed store (`@effect/platform`) that writes a JSON file and
  derives a stream of **change events** (`TodoUpserted` / `TodoRemoved`) from its `SubscriptionRef`, so
  every renderer stays live without polling.
- Every kind of RPC: query, mutation, typed error, **binary** (`Uint8Array` export), and a
  **server-stream of change events** the renderer folds into the live list.
- A renderer built with **effect-atom**: a `TodosApi` `Context.Tag` consumed by atoms, mockable in
  tests, backed in production by the RPC client.
- **Full test coverage** with `@effect/vitest` — contract/schema round-trips, end-to-end transport,
  the filesystem store, the handlers (via `RpcTest`), and the renderer atoms + a React render test.

## Architecture

```
┌───────────────────────────────── Electron main (Node) ─────────────────────────────────┐
│  ManagedRuntime( Live )                                                                │
│    RpcServer.layer(TodosRpcs)  ◄── TodosHandlersLive ◄── TodosRepo                     │
│            │                              (FileSystem + SubscriptionRef                │
│            ▼                               → userData/todos.json)                      │
│    layerIpcServer  ── RpcServer.Protocol over IpcServerPort                            │
│                       (+ RpcPortHandoff.bind, rebinds a fresh port per load)           │
└────────────────────────────────────────────────────────────────────────────────────────┘
                   MessagePortMain │ (new MessageChannelMain per load)
                                            ▼
                                 preload (isolated world)
                  ipcRenderer.on("rpc-port") → window.postMessage(port)
                                            │
                                            ▼
┌─────────────────────────────── Electron renderer (DOM) ────────────────────────────────┐
│  window message "rpc-port" → MessagePort  (listener attached at startup)               │
│    TodosApiLive:                                                                       │
│      layerIpcClient ── RpcClient.Protocol over IpcClientPort                           │
│      RpcClient.make(TodosRpcs) ── adapted to ── TodosApi (Context.Tag)                 │
│            │                                                                           │
│            ▼                                                                           │
│    atoms.ts:  Atom.runtime(TodosApiLive) ── effect-atom ── React (App.tsx)             │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

The renderer never sees the transport: it depends only on the `TodosApi` tag. Production binds the
concrete `TodosApiLive` layer (RPC client over the handed-off `MessagePort`); tests override the atom
runtime's layer with a mock `TodosApi` (`Atom.initialValue(runtime.layer, mockLayer)`).

### Why MsgPack + `supportsTransferables: false`

Each `postMessage` carries one complete MsgPack frame. Electron's `MessagePortMain` cannot transfer
`ArrayBuffer`s ([electron#34905](https://github.com/electron/electron/issues/34905)), so the transports
declare `supportsTransferables: false` and binary payloads ride as MsgPack-copied bytes.

### Why a port is handed off per load

`RpcServer` runs for the app's lifetime inside a `ManagedRuntime`. On every `did-finish-load` the main
process mints a new `MessageChannelMain`, sends one port to the renderer, and `bind`s the other to the
long-lived server. Binding a new port interrupts the previous load's in-flight server streams, so a
reloaded renderer never receives stale pushes.

## Project structure

```
src/
  shared/                  # the contract, shared by both processes
    todos-rpc.ts           # Todo/TodoId/TodoTitle + TodoNotFound/TodosError + TodosRpcs
    rpc-client.ts          # layerIpcClient — renderer RpcClient.Protocol over a MessagePort
  main/
    ipc-server.ts          # layerIpcServer + RpcPortHandoff — main RpcServer.Protocol
    todos-repo.ts          # FileSystem + SubscriptionRef store (Effect.Service), userData path
    todos-handlers.ts      # TodosRpcs.toLayer(...) — handlers wired to the repo
    index.ts               # composes the Live layer + Electron bootstrap (windows, lifecycle)
  preload/
    index.ts               # relays the transferred port into the renderer's main world
  renderer/
    todos-api.ts           # TodosApi (Context.Tag) — what the atoms consume
    todos-api-live.ts      # layerTodosApi (mapping) + TodosApiLive (wired to the port)
    atoms.ts               # Atom.runtime(TodosApiLive) — folds the change-event stream into a list + fn atoms
    App.tsx                # the React UI (imports the atoms directly)
    main.tsx               # renderer bootstrap (mounts React in a RegistryProvider)
    index.html
```

## Running

```sh
pnpm install
pnpm dev         # launch the Electron app with HMR
pnpm build       # bundle main + preload + renderer to out/
pnpm typecheck   # tsc --noEmit
pnpm test        # run the vitest suite
```

> The todos file is written to `app.getPath("userData")/todos.json`.

## Tests

Tests live next to the code they cover (no `__tests__/` directories).

| File | What it covers | How |
| --- | --- | --- |
| `shared/rpc-contract.test.ts` | RPC tag inventory; MsgPack `Uint8Array` + `Todo` round-trips; tagged-error equality | `RpcSerialization.layerMsgPack` |
| `main/transport.test.ts` | unary, binary, typed error, stream ordering, **port-swap interruption**, **defect isolation**, malformed-frame resilience | client ⇄ server over a `node:worker_threads` `MessageChannel` |
| `main/todos-repo.test.ts` | create/toggle/remove/list/export, persistence across reload, corrupt-file + write failures, the upsert/removal `changes` event stream | real `NodeFileSystem` + a temp dir per test (Electron's `app.getPath` mocked) |
| `main/todos-handlers.test.ts` | the full handler chain end to end | `RpcTest.makeClient` (in-memory, no transport) |
| `renderer/atoms.test.ts` | folding the change-event stream into the list, typed failure propagation, concurrent mutation fns | mock `TodosApi` layer + `Registry` |
| `renderer/todos-api-live.test.ts` | the production `layerTodosApi` delegates each method to the right RPC | real transport over a `MessageChannel` |
| `renderer/App.test.tsx` | UI rendering, empty/failure states, add/toggle/delete/export | `@testing-library/react` + happy-dom |
| `renderer/App.rejection.test.tsx` | a failed mutation does not leak an unhandled rejection | `@testing-library/react` + happy-dom |

The transport, repo, and handler tests need no Electron runtime — the renderer's port is modelled by a
Node `MessageChannel`, and the file store uses a real temp directory.

## Versions

Pinned to a known-good Effect set: `effect` 3.21.3, `@effect/rpc` 0.75.1, `@effect/platform` 0.96.1,
`@effect/platform-node` 0.107.0, `@effect-atom/atom` 0.5.3, `@effect-atom/atom-react` 0.5.0. Built with
Electron 41 and electron-vite 2.
