import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { type RpcClient, type RpcGroup, RpcTest } from "@effect/rpc"
import { describe, expect, it } from "@effect/vitest"
import { Chunk, Deferred, Effect, Fiber, Stream } from "effect"
import { afterEach, beforeEach, vi } from "vitest"
import { TodosRpcs, TodoRemoved, type TodoId, TodoUpserted } from "../shared/todos-rpc"
import { TodosRepo } from "./todos-repo"
import { TodosHandlersLive } from "./todos-handlers"

// TodosRepo hard-codes its file under Electron's userData dir; point app.getPath
// at a fresh temp dir per test (vi.mock is hoisted above the imports above).
const electron = vi.hoisted(() => ({ userDataDir: "" }))
vi.mock("electron", () => ({ app: { getPath: () => electron.userDataDir } }))

beforeEach(() => {
  electron.userDataDir = mkdtempSync(join(tmpdir(), "todos-handlers-"))
})
afterEach(() => {
  rmSync(electron.userDataDir, { recursive: true, force: true })
})

type TodosClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof TodosRpcs>>

const withClient = <A, E>(use: (client: TodosClient) => Effect.Effect<A, E>) =>
  Effect.gen(function*() {
    const client = yield* RpcTest.makeClient(TodosRpcs).pipe(
      Effect.provide(TodosHandlersLive),
      Effect.provide(TodosRepo.Default)
    )
    return yield* use(client)
  }).pipe(Effect.scoped)

describe("TodosRpcs handlers (in-memory RpcTest client)", () => {
  it.effect("create then list round-trips through the handlers", () =>
    withClient((client) =>
      Effect.gen(function*() {
        const created = yield* client.TodoCreate({ title: "Buy milk" })
        expect(created.title).toBe("Buy milk")
        expect(yield* client.TodosList()).toStrictEqual([created])
      })))

  it.effect("toggle updates completion through the handlers", () =>
    withClient((client) =>
      Effect.gen(function*() {
        const created = yield* client.TodoCreate({ title: "Task" })
        const toggled = yield* client.TodoToggle({ id: created.id })
        expect(toggled.completed).toBe(true)
      })))

  it.effect("toggle surfaces TodoNotFound as a typed error", () =>
    withClient((client) =>
      Effect.gen(function*() {
        const error = yield* Effect.flip(client.TodoToggle({ id: "missing" as TodoId }))
        expect(error._tag).toBe("TodoNotFound")
      })))

  it.effect("remove deletes a todo through the handlers", () =>
    withClient((client) =>
      Effect.gen(function*() {
        const created = yield* client.TodoCreate({ title: "Delete me" })
        yield* client.TodoRemove({ id: created.id })
        expect(yield* client.TodosList()).toStrictEqual([])
      })))

  it.effect("export returns the persisted JSON bytes through the handlers", () =>
    withClient((client) =>
      Effect.gen(function*() {
        yield* client.TodoCreate({ title: "Exported" })
        const bytes = yield* client.TodosExport()
        expect(bytes).toBeInstanceOf(Uint8Array)
        expect(new TextDecoder().decode(bytes)).toContain("Exported")
      })))

  it.effect("streams upsert and removal events to a TodosWatch subscriber", () =>
    withClient((client) =>
      Effect.gen(function*() {
        const seed = yield* client.TodoCreate({ title: "Seed" })
        const subscribed = yield* Deferred.make<void>()
        const fiber = yield* client.TodosWatch().pipe(
          Stream.tap(() => Deferred.succeed(subscribed, undefined)),
          Stream.take(3),
          Stream.runCollect,
          Effect.fork
        )
        yield* Deferred.await(subscribed)
        const created = yield* client.TodoCreate({ title: "Streamed" })
        yield* client.TodoRemove({ id: created.id })
        const events = Chunk.toReadonlyArray(yield* Fiber.join(fiber))
        expect(events).toStrictEqual([
          TodoUpserted.make({ todo: seed }),
          TodoUpserted.make({ todo: created }),
          TodoRemoved.make({ id: created.id })
        ])
      })))
})
