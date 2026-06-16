import { type MessagePort } from "node:worker_threads"
import { Effect, Stream } from "effect"
import { type IpcClientPort } from "../shared/rpc-client"
import { Todo, type TodoId } from "../shared/todos-rpc"
import { type IpcServerPort } from "./ipc-server"

export const TODO_A = new Todo({ id: "a" as TodoId, title: "First", completed: false, createdAt: 1 })
export const TODO_B = new Todo({ id: "b" as TodoId, title: "Second", completed: true, createdAt: 2 })
export const STUB_BYTES = new Uint8Array([0, 1, 2, 254, 255])

// A full handler record (toLayer requires every tag). Tests spread overrides
// over this; unused handlers are Effect.never / Stream.empty.
export const baseHandlers = () => ({
  TodosList: () => Effect.succeed<ReadonlyArray<Todo>>([]),
  TodoCreate: () => Effect.never,
  TodoToggle: () => Effect.never,
  TodoRemove: () => Effect.never,
  TodosExport: () => Effect.succeed(new Uint8Array()),
  TodosWatch: () => Stream.empty
})

export function clientAdapter(port: MessagePort): IpcClientPort {
  const adapter: IpcClientPort = {
    onmessage: null,
    postMessage: (message) => port.postMessage(message),
    start: () => port.start(),
    close: () => port.close()
  }
  port.on("message", (data: string | Uint8Array) => {
    if (adapter.onmessage) {
      adapter.onmessage({ data })
    }
  })
  return adapter
}

export function serverAdapter(port: MessagePort): IpcServerPort {
  return {
    on: (event: string, listener: (event: { data: string | Uint8Array }) => void) => {
      if (event === "message") {
        port.on("message", (data: string | Uint8Array) => listener({ data }))
      } else {
        port.on("close", () => (listener as unknown as () => void)())
      }
    },
    off: () => {
      // Single-client tests never swap the bound port mid-test.
    },
    postMessage: (message: string | Uint8Array) => port.postMessage(message),
    start: () => port.start(),
    close: () => port.close()
  } as unknown as IpcServerPort
}
