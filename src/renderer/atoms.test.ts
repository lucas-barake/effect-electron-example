import { Atom, Registry } from "@effect-atom/atom"
import { RpcClientError } from "@effect/rpc/RpcClientError"
import { describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Layer, Stream } from "effect"
import { Todo, TodoUpserted, type TodoId } from "../shared/todos-rpc"
import { create, runtime, todos, toggle } from "./atoms"
import { TodosApi } from "./todos-api"

const createTestTodo = (
  overrides: Partial<{ id: TodoId; title: string; completed: boolean; createdAt: number }> = {}
): Todo =>
  new Todo({
    id: overrides.id ?? ("test-id" as TodoId),
    title: overrides.title ?? "Test",
    completed: overrides.completed ?? false,
    createdAt: overrides.createdAt ?? 0
  })

const makeApiMock = (options?: { todos?: ReadonlyArray<Todo>; changesFails?: boolean }) => {
  const calls: Array<{ method: string; args: unknown }> = []
  const items = options?.todos ?? []
  const layer = Layer.succeed(
    TodosApi,
    TodosApi.of({
      list: () => {
        calls.push({ method: "list", args: undefined })
        return Effect.succeed(items)
      },
      create: (title) => {
        calls.push({ method: "create", args: title })
        return Effect.succeed(createTestTodo({ title }))
      },
      toggle: (id) => {
        calls.push({ method: "toggle", args: id })
        return Effect.succeed(createTestTodo({ id, completed: true }))
      },
      remove: (id) => {
        calls.push({ method: "remove", args: id })
        return Effect.void
      },
      exportBytes: () => {
        calls.push({ method: "exportBytes", args: undefined })
        return Effect.succeed(new TextEncoder().encode("[]"))
      },
      changes: () =>
        options?.changesFails
          ? Stream.fail(new RpcClientError({ reason: "Protocol", message: "boom" }))
          : Stream.fromIterable(items.map((todo) => TodoUpserted.make({ todo })))
    })
  )
  return { layer, calls }
}

// A fresh Registry per test, with the atom runtime's layer overridden to a mock
// TodosApi — the module-level atoms run unchanged against the substituted layer.
const mock = (layer: Layer.Layer<TodosApi>) => Registry.make({ initialValues: [Atom.initialValue(runtime.layer, layer)] })

describe("desktop todos atoms (the TodosApi Context.Tag is mockable in tests)", () => {
  it("resolves the todos atom from the changes stream through a mock TodosApi", async () => {
    const items = [createTestTodo({ title: "A" }), createTestTodo({ id: "2" as TodoId, title: "B" })]
    const { layer } = makeApiMock({ todos: items })
    const registry = mock(layer)

    const result = await Effect.runPromise(
      Registry.getResult(registry, todos, { suspendOnWaiting: true })
    )

    expect(result).toStrictEqual(items)
  })

  it("surfaces a failing changes stream as a typed atom Failure", async () => {
    const { layer } = makeApiMock({ changesFails: true })
    const registry = mock(layer)

    // Effect.flip succeeds only on the TYPED error channel — proving the atom
    // propagates the tagged failure (not a defect).
    const error = await Effect.runPromise(
      Effect.flip(Registry.getResult(registry, todos, { suspendOnWaiting: true }))
    )

    expect(error._tag).toBe("RpcClientError")
  })

  it("runs the create fn atom through the mock api and records the call", async () => {
    const { calls, layer } = makeApiMock()
    const registry = mock(layer)

    registry.set(create, "New todo")
    const created = await Effect.runPromise(
      Registry.getResult(registry, create, { suspendOnWaiting: true })
    )

    expect(created.title).toBe("New todo")
    expect(calls).toContainEqual({ method: "create", args: "New todo" })
  })

  it("runs the toggle fn atom through the mock api", async () => {
    const { calls, layer } = makeApiMock()
    const registry = mock(layer)

    registry.set(toggle, "test-id" as TodoId)
    const toggled = await Effect.runPromise(
      Registry.getResult(registry, toggle, { suspendOnWaiting: true })
    )

    expect(toggled.completed).toBe(true)
    expect(calls).toContainEqual({ method: "toggle", args: "test-id" })
  })

  it("runs both mutations when two rows are toggled in quick succession (concurrent fn)", async () => {
    const completed: Array<string> = []
    const gate = await Effect.runPromise(Deferred.make<void>())
    const layer = Layer.succeed(
      TodosApi,
      TodosApi.of({
        list: () => Effect.succeed([]),
        create: (title) => Effect.succeed(createTestTodo({ title })),
        toggle: (id) =>
          Effect.gen(function*() {
            // The first call blocks until the gate opens; a non-concurrent fn
            // atom would interrupt it when the second call arrives.
            if (id === ("A" as TodoId)) yield* Deferred.await(gate)
            completed.push(id as string)
            return createTestTodo({ id, completed: true })
          }),
        remove: () => Effect.void,
        exportBytes: () => Effect.succeed(new TextEncoder().encode("[]")),
        changes: () => Stream.empty
      })
    )
    const registry = mock(layer)
    const unmount = registry.mount(toggle)

    registry.set(toggle, "A" as TodoId)
    registry.set(toggle, "B" as TodoId)
    await Effect.runPromise(Deferred.succeed(gate, undefined))
    await Effect.runPromise(Effect.sleep("50 millis"))
    unmount()

    expect(completed).toContain("A")
    expect(completed).toContain("B")
  })
})
