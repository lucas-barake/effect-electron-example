import { MessageChannel } from "node:worker_threads"
import { RpcClient, RpcSerialization, RpcServer } from "@effect/rpc"
import { describe, expect, it } from "@effect/vitest"
import { Cause, Chunk, Effect, Exit, Layer, Stream } from "effect"
import { TodoNotFound, TodosRpcs, Todo, TodoUpserted, type TodoId } from "../shared/todos-rpc"
import { type IpcClientPort, layerIpcClient, makeIpcClientProtocol } from "../shared/rpc-client"
import { layerIpcServer, RpcPortHandoff } from "./ipc-server"
import { baseHandlers, clientAdapter, serverAdapter, STUB_BYTES, TODO_A, TODO_B } from "./harness"

const serverConfig = { disableFatalDefects: true } as const

const makeServer = <H extends Parameters<typeof TodosRpcs.of>[0]>(handlers: H) =>
  RpcServer.layer(TodosRpcs, serverConfig).pipe(
    Layer.provide(TodosRpcs.toLayer(handlers)),
    Layer.provideMerge(layerIpcServer)
  )

describe("layerIpc transport (client ⇄ server over a MessagePort)", () => {
  it("round-trips a unary call and a binary Uint8Array end-to-end", async () => {
    const channel = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodosList: () => Effect.succeed([TODO_A, TODO_B]),
      TodosExport: () => Effect.succeed(STUB_BYTES)
    })
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const client = yield* RpcClient.make(TodosRpcs)
      const todos = yield* client.TodosList()
      const bytes = yield* client.TodosExport()
      return { todos, bytes }
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    const { bytes, todos } = await Effect.runPromise(program)

    expect(todos).toStrictEqual([TODO_A, TODO_B])
    expect(bytes).toBeInstanceOf(Uint8Array)
    expect(Array.from(bytes)).toEqual(Array.from(STUB_BYTES))

    channel.port1.close()
    channel.port2.close()
  })

  it("round-trips a handler failure as a typed tagged error over the wire", async () => {
    const channel = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodoToggle: ({ id }) => Effect.fail(new TodoNotFound({ id }))
    })
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const client = yield* RpcClient.make(TodosRpcs)
      // Effect.flip succeeds only on the TYPED error channel (a defect would not
      // flip), proving the tagged error round-tripped.
      return yield* Effect.flip(client.TodoToggle({ id: "missing" as TodoId }))
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    const error = await Effect.runPromise(program)

    expect(error._tag).toBe("TodoNotFound")
    expect((error as TodoNotFound).id).toBe("missing")

    channel.port1.close()
    channel.port2.close()
  })

  it("rejects a blank title at the payload boundary (NonEmptyTrimmedString)", async () => {
    const channel = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodoCreate: ({ title }) => Effect.succeed(new Todo({ id: "x" as TodoId, title, completed: false, createdAt: 0 }))
    })
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const client = yield* RpcClient.make(TodosRpcs)
      return yield* client.TodoCreate({ title: "   " }).pipe(Effect.exit)
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    const exit = await Effect.runPromise(program)

    expect(Exit.isFailure(exit)).toBe(true)

    channel.port1.close()
    channel.port2.close()
  })

  it("delivers a server-streamed sequence of values in order", async () => {
    const channel = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodosWatch: () => Stream.fromIterable([TodoUpserted.make({ todo: TODO_A }), TodoUpserted.make({ todo: TODO_B })])
    })
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const client = yield* RpcClient.make(TodosRpcs)
      const collected = yield* Stream.runCollect(client.TodosWatch())
      return Chunk.toReadonlyArray(collected)
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    const emissions = await Effect.runPromise(program)

    expect(emissions).toStrictEqual([TodoUpserted.make({ todo: TODO_A }), TodoUpserted.make({ todo: TODO_B })])

    channel.port1.close()
    channel.port2.close()
  })

  it("ships a handler defect to the client without poisoning concurrent requests (disableFatalDefects)", async () => {
    let resolveFirst!: () => void
    const gotFirst = new Promise<void>((resolve) => {
      resolveFirst = resolve
    })
    let streamErrored = false

    const channel = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodoCreate: () => Effect.die("boom"),
      TodosList: () => Effect.succeed([TODO_A]),
      TodosWatch: () => Stream.make(TodoUpserted.make({ todo: TODO_A })).pipe(Stream.concat(Stream.never))
    })
    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel.port2))
      const client = yield* RpcClient.make(TodosRpcs)

      // A concurrent, long-lived request that must outlive another request's defect.
      const watcher = yield* Effect.forkScoped(
        Stream.runForEach(client.TodosWatch(), () => Effect.sync(() => resolveFirst())).pipe(
          Effect.catchAllCause(() =>
            Effect.sync(() => {
              streamErrored = true
            })
          )
        )
      )
      yield* Effect.promise(() => gotFirst)

      const cause = yield* client.TodoCreate({ title: "x" }).pipe(Effect.sandbox, Effect.flip)

      // With disableFatalDefects:false the defect would clear ALL pending entries
      // (failing the watcher); with true only this request fails and new calls work.
      yield* Effect.sleep("100 millis")
      const after = yield* client.TodosList()

      return { after, cause, running: watcher.unsafePoll() === null }
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    const { after, cause, running } = await Effect.runPromise(program)

    expect(Cause.pretty(cause)).toContain("boom")
    expect(streamErrored).toBe(false)
    expect(running).toBe(true)
    expect(after).toStrictEqual([TODO_A])

    channel.port1.close()
    channel.port2.close()
  })

  it("interrupts the previous client's in-flight server stream on a port swap", async () => {
    let resolveFirst!: () => void
    const gotFirst = new Promise<void>((resolve) => {
      resolveFirst = resolve
    })
    let wasInterrupted = false
    let resolveInterrupted!: () => void
    const interrupted = new Promise<void>((resolve) => {
      resolveInterrupted = resolve
    })

    const channel1 = new MessageChannel()
    const channel2 = new MessageChannel()
    const server = makeServer({
      ...baseHandlers(),
      TodosWatch: () =>
        Stream.make(TodoUpserted.make({ todo: TODO_A })).pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(
            Effect.sync(() => {
              wasInterrupted = true
              resolveInterrupted()
            })
          )
        )
    })

    const program = Effect.gen(function*() {
      const handoff = yield* RpcPortHandoff
      handoff.bind(serverAdapter(channel1.port2))
      const client = yield* RpcClient.make(TodosRpcs)

      yield* Effect.forkScoped(
        Stream.runForEach(client.TodosWatch(), () => Effect.sync(() => resolveFirst()))
      )
      yield* Effect.promise(() => gotFirst)

      // Swap in a fresh port: the old client is offered to `disconnects`, which
      // RpcServer drains to interrupt its in-flight TodosWatch fiber.
      handoff.bind(serverAdapter(channel2.port2))

      yield* Effect.promise(() => interrupted).pipe(
        Effect.timeoutFail({
          duration: "5 seconds",
          onTimeout: () => new Error("in-flight server stream was not interrupted on swap")
        })
      )
    }).pipe(
      Effect.provide(layerIpcClient(clientAdapter(channel1.port1))),
      Effect.provide(server),
      Effect.scoped
    )

    await Effect.runPromise(program)

    expect(wasInterrupted).toBe(true)

    channel1.port1.close()
    channel1.port2.close()
    channel2.port1.close()
    channel2.port2.close()
  })

  it("client transport swallows a parser decode error instead of throwing in the message callback", async () => {
    const throwingSerialization = Layer.succeed(
      RpcSerialization.RpcSerialization,
      RpcSerialization.RpcSerialization.of({
        contentType: "application/x-broken",
        includesFraming: true,
        unsafeMake: () => ({
          decode: () => {
            throw new Error("bad frame")
          },
          encode: () => undefined
        })
      })
    )
    let handler: ((event: { data: string | Uint8Array }) => void) | null = null
    const fakePort: IpcClientPort = {
      get onmessage() {
        return handler
      },
      set onmessage(next) {
        handler = next
      },
      postMessage: () => {},
      start: () => {},
      close: () => {}
    }

    await Effect.runPromise(
      Effect.gen(function*() {
        yield* makeIpcClientProtocol(fakePort)
        expect(handler).not.toBeNull()
        expect(() => handler?.({ data: new Uint8Array([1, 2, 3]) })).not.toThrow()
      }).pipe(Effect.provide(throwingSerialization), Effect.scoped)
    )
  })
})
