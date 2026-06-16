import { RpcClient } from "@effect/rpc"
import { Effect, Layer } from "effect"
import { type IpcClientPort, layerIpcClient } from "../shared/rpc-client"
import { TodosRpcs } from "../shared/todos-rpc"
import { TodosApi } from "./todos-api"

// TodosApi backed by the @effect/rpc client. It requires only RpcClient.Protocol,
// so production wires the real MessagePort transport (below) while tests provide
// an in-memory MessageChannel — the method-to-RPC mapping is exercised either way.
export const layerTodosApi: Layer.Layer<TodosApi, never, RpcClient.Protocol> = Layer.scoped(
  TodosApi,
  Effect.gen(function*() {
    const client = yield* RpcClient.make(TodosRpcs)
    return TodosApi.of({
      list: () => client.TodosList(),
      create: (title) => client.TodoCreate({ title }),
      toggle: (id) => client.TodoToggle({ id }),
      remove: (id) => client.TodoRemove({ id }),
      exportBytes: () => client.TodosExport(),
      changes: () => client.TodosWatch()
    })
  })
)

// Start listening for main's handed-off port the instant this module loads —
// before React mounts the atoms — so a port delivered on did-finish-load is
// never missed. Guarded for the node test env, which overrides the atom runtime
// layer (see atoms.ts) and never builds this transport.
let resolvePort!: (port: MessagePort) => void
const portReady = new Promise<MessagePort>((resolve) => {
  resolvePort = resolve
})
if (typeof window !== "undefined") {
  const onMessage = (event: MessageEvent) => {
    if (event.data === "rpc-port" && event.ports[0] !== undefined) {
      window.removeEventListener("message", onMessage)
      resolvePort(event.ports[0])
    }
  }
  window.addEventListener("message", onMessage)
}

// Route onmessage straight to the underlying DOM port via a setter, so the
// transport owns the single handler and no frame can land before it attaches.
export const toClientPort = (port: MessagePort): IpcClientPort => ({
  get onmessage() {
    return port.onmessage as IpcClientPort["onmessage"]
  },
  set onmessage(handler: IpcClientPort["onmessage"]) {
    port.onmessage = handler === null ? null : (event) => handler({ data: event.data })
  },
  postMessage: (message) => port.postMessage(message),
  start: () => port.start(),
  close: () => port.close()
})

const layerRpcClient: Layer.Layer<RpcClient.Protocol> = Layer.unwrapEffect(
  Effect.promise(() => portReady).pipe(Effect.map((port) => layerIpcClient(toClientPort(port))))
)

export const TodosApiLive: Layer.Layer<TodosApi> = layerTodosApi.pipe(Layer.provide(layerRpcClient))
