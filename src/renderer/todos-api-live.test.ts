import { MessageChannel } from "node:worker_threads"
import { RpcServer } from "@effect/rpc"
import { describe, expect, it } from "@effect/vitest"
import { Chunk, Effect, Layer, Stream } from "effect"
import { vi } from "vitest"
import { baseHandlers, clientAdapter, serverAdapter } from "../main/harness"
import { layerIpcServer, RpcPortHandoff } from "../main/ipc-server"
import { layerIpcClient } from "../shared/rpc-client"
import { TodosRpcs, Todo, TodoUpserted, type TodoId } from "../shared/todos-rpc"
import { TodosApi } from "./todos-api"
import { layerTodosApi, toClientPort } from "./todos-api-live"

// Distinguishable per-method results prove each TodosApi method delegates to the
// correct RPC (a swapped mapping would return the wrong value or fail to typecheck).
const LISTED = new Todo({ id: "listed" as TodoId, title: "Listed", completed: false, createdAt: 1 })
const CREATED = new Todo({ id: "created" as TodoId, title: "Created", completed: false, createdAt: 2 })
const TOGGLED = new Todo({ id: "toggled" as TodoId, title: "Toggled", completed: true, createdAt: 3 })
const EXPORTED = new Uint8Array([9, 8, 7])

const handlers = {
  ...baseHandlers(),
  TodosList: () => Effect.succeed([LISTED]),
  TodoCreate: () => Effect.succeed(CREATED),
  TodoToggle: () => Effect.succeed(TOGGLED),
  TodoRemove: () => Effect.void,
  TodosExport: () => Effect.succeed(EXPORTED),
  TodosWatch: () => Stream.make(TodoUpserted.make({ todo: LISTED }))
}

describe("layerTodosApi (production renderer wiring over the transport)", () => {
  it("delegates each TodosApi method to its RPC over the MessagePort transport", async () => {
    const channel = new MessageChannel()
    const server = RpcServer.layer(TodosRpcs, { disableFatalDefects: true }).pipe(
      Layer.provide(TodosRpcs.toLayer(handlers)),
      Layer.provideMerge(layerIpcServer)
    )
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const api = yield* TodosApi
      const list = yield* api.list()
      const created = yield* api.create("anything")
      const toggled = yield* api.toggle("id" as TodoId)
      const bytes = yield* api.exportBytes()
      const watched = Chunk.toReadonlyArray(yield* Stream.runCollect(Stream.take(api.changes(), 1)))
      yield* api.remove("id" as TodoId)
      return { bytes, created, list, toggled, watched }
    }).pipe(
      Effect.provide(layerTodosApi.pipe(Layer.provide(layerIpcClient(clientAdapter(channel.port1))))),
      Effect.provide(server),
      Effect.scoped
    )

    const { bytes, created, list, toggled, watched } = await Effect.runPromise(program)

    expect(list).toStrictEqual([LISTED])
    expect(created).toStrictEqual(CREATED)
    expect(toggled).toStrictEqual(TOGGLED)
    expect(Array.from(bytes)).toEqual([9, 8, 7])
    expect(watched).toStrictEqual([TodoUpserted.make({ todo: LISTED })])

    channel.port1.close()
    channel.port2.close()
  })
})

describe("toClientPort (adapts the handed-off DOM MessagePort to IpcClientPort)", () => {
  it("forwards inbound messages as { data } and delegates postMessage/start/close", () => {
    const underlying = {
      onmessage: null as ((event: { data: unknown }) => void) | null,
      postMessage: vi.fn(),
      start: vi.fn(),
      close: vi.fn()
    }
    const client = toClientPort(underlying as unknown as MessagePort)

    const received: Array<unknown> = []
    client.onmessage = (event) => received.push(event.data)
    // A frame arriving on the raw DOM port is re-shaped to { data } for the transport.
    underlying.onmessage?.({ data: "frame" })
    expect(received).toEqual(["frame"])

    client.postMessage("out")
    expect(underlying.postMessage).toHaveBeenCalledWith("out")
    client.start()
    expect(underlying.start).toHaveBeenCalledTimes(1)
    client.close()
    expect(underlying.close).toHaveBeenCalledTimes(1)

    client.onmessage = null
    expect(underlying.onmessage).toBeNull()
  })
})
