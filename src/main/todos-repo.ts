import { FileSystem } from "@effect/platform"
import type { PlatformError } from "@effect/platform/Error"
import { NodeFileSystem } from "@effect/platform-node"
import { app } from "electron"
import { randomUUID } from "node:crypto"
import { join } from "node:path"
import { Clock, Effect, Equal, Option, Schema, Stream, SubscriptionRef } from "effect"
import type { ParseError } from "effect/ParseResult"
import { Todo, type TodoEvent, TodoNotFound, TodoRemoved, TodosError, TodoUpserted, type TodoId } from "../shared/todos-rpc"

const TodosFromJson = Schema.parseJson(Schema.Array(Todo))

const toTodosError = (writeReason: "ReadFailed" | "WriteFailed", error: PlatformError | ParseError): TodosError =>
  error._tag === "ParseError"
    ? new TodosError({ reason: "Corrupted", detail: "The stored todos file could not be parsed." })
    : new TodosError({ reason: writeReason, detail: `A filesystem error occurred (${error._tag}).` })

// Derive the change events between two list snapshots. Unchanged todos keep their
// instance across a mutation (toggle/remove rebuild the array but reuse the rest),
// so reference identity is enough to emit one event per actual change.
const diffEvents = (previous: Option.Option<ReadonlyArray<Todo>>, current: ReadonlyArray<Todo>): ReadonlyArray<TodoEvent> => {
  const before = Option.getOrElse(previous, (): ReadonlyArray<Todo> => [])
  const beforeById = new Map(before.map((todo) => [todo.id, todo] as const))
  const currentIds = new Set(current.map((todo) => todo.id))
  const events: Array<TodoEvent> = []
  for (const todo of before) {
    if (!currentIds.has(todo.id)) events.push(TodoRemoved.make({ id: todo.id }))
  }
  for (const todo of current) {
    const old = beforeById.get(todo.id)
    if (old === undefined || !Equal.equals(old, todo)) events.push(TodoUpserted.make({ todo }))
  }
  return events
}

export class TodosRepo extends Effect.Service<TodosRepo>()("main/TodosRepo", {
  dependencies: [NodeFileSystem.layer],
  effect: Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem
    const directory = app.getPath("userData")
    const filePath = join(directory, "todos.json")

    const decodeTodos = Schema.decode(TodosFromJson)
    const encodeTodos = Schema.encode(TodosFromJson)

    const initial = yield* fs.readFileString(filePath).pipe(
      Effect.flatMap(decodeTodos),
      Effect.catchIf(
        (error) => error._tag === "SystemError" && error.reason === "NotFound",
        () => Effect.succeed<ReadonlyArray<Todo>>([])
      ),
      Effect.mapError((error) => toTodosError("ReadFailed", error)),
      Effect.withSpan("TodosRepo.load")
    )

    const ref = yield* SubscriptionRef.make(initial)

    const persist = (todos: ReadonlyArray<Todo>) =>
      Effect.gen(function*() {
        const json = yield* encodeTodos(todos)
        yield* fs.makeDirectory(directory, { recursive: true })
        yield* fs.writeFileString(filePath, json)
      }).pipe(
        Effect.mapError((error) => toTodosError("WriteFailed", error)),
        Effect.withSpan("TodosRepo.persist")
      )

    // SubscriptionRef is a Synchronized ref: modifyEffect/updateEffect run the
    // read → persist → set+publish atomically under its own lock, so concurrent
    // mutations are serialized and persist always lands before the change is
    // published (a failed persist sets nothing and publishes nothing).
    const create = Effect.fn("TodosRepo.create")((title: string) =>
      SubscriptionRef.modifyEffect(ref, (todos) =>
        Effect.gen(function*() {
          const createdAt = yield* Clock.currentTimeMillis
          const todo = new Todo({ id: randomUUID() as TodoId, title, completed: false, createdAt })
          const next = [...todos, todo]
          yield* persist(next)
          return [todo, next] as const
        })))

    const toggle = Effect.fn("TodosRepo.toggle")((id: TodoId) =>
      SubscriptionRef.modifyEffect(ref, (todos) =>
        Effect.gen(function*() {
          const existing = todos.find((todo) => todo.id === id)
          if (existing === undefined) return yield* Effect.fail(new TodoNotFound({ id }))
          const updated = new Todo({
            id: existing.id,
            title: existing.title,
            completed: !existing.completed,
            createdAt: existing.createdAt
          })
          const next = todos.map((todo) => (todo.id === id ? updated : todo))
          yield* persist(next)
          return [updated, next] as const
        })))

    const remove = Effect.fn("TodosRepo.remove")((id: TodoId) =>
      SubscriptionRef.updateEffect(ref, (todos) =>
        Effect.gen(function*() {
          if (!todos.some((todo) => todo.id === id)) return yield* Effect.fail(new TodoNotFound({ id }))
          const next = todos.filter((todo) => todo.id !== id)
          yield* persist(next)
          return next
        })))

    const exportBytes = fs.readFile(filePath).pipe(
      Effect.catchIf(
        (error) => error._tag === "SystemError" && error.reason === "NotFound",
        () => Effect.succeed(new TextEncoder().encode("[]"))
      ),
      Effect.mapError((error) => toTodosError("ReadFailed", error)),
      Effect.withSpan("TodosRepo.exportBytes")
    )

    // SubscriptionRef.changes is gap-free (current value, then every update), so
    // diffing consecutive snapshots yields a correct delta stream by construction
    // — the first pair is [None, current], replaying the current list as upserts.
    const changes = ref.changes.pipe(
      Stream.zipWithPrevious,
      Stream.mapConcat(([previous, current]) => diffEvents(previous, current))
    )

    return {
      list: SubscriptionRef.get(ref),
      changes,
      create,
      toggle,
      remove,
      exportBytes
    } as const
  })
}) {}
