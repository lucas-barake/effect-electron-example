import { Atom } from "@effect-atom/atom"
import { Effect, Stream } from "effect"
import { type Todo, type TodoEvent } from "../shared/todos-rpc"
import { TodosApi } from "./todos-api"
import { TodosApiLive } from "./todos-api-live"

// The atom runtime is built from the concrete RPC-backed layer. Tests swap the
// implementation by overriding `runtime.layer` in a fresh Registry — e.g.
// Registry.make({ initialValues: [Atom.initialValue(runtime.layer, mockLayer)] }).
export const runtime = Atom.runtime(TodosApiLive)

// Effect.serviceFunctions derives one accessor per Effect-returning method of the
// service, so each is `(...args) => Effect<…, TodosApi>` — no manual flatMap.
const api = Effect.serviceFunctions(TodosApi)

const applyEvent = (todos: ReadonlyArray<Todo>, event: TodoEvent): ReadonlyArray<Todo> => {
  switch (event._tag) {
    case "TodoUpserted":
      return todos.some((todo) => todo.id === event.todo.id)
        ? todos.map((todo) => (todo.id === event.todo.id ? event.todo : todo))
        : [...todos, event.todo]
    case "TodoRemoved":
      return todos.filter((todo) => todo.id !== event.id)
  }
}

// The watch streams individual change events; fold them into the live list so the
// UI renders an array and stays in sync after every mutation with no refetch.
export const todos = runtime.atom(
  Stream.unwrap(Effect.map(TodosApi, (svc) => svc.changes())).pipe(
    Stream.scan([] as ReadonlyArray<Todo>, applyEvent)
  )
)

// concurrent: true so independent row mutations run on their own fibers; the
// default (non-concurrent) interrupts an in-flight call when the next arrives,
// which would drop a mutation when two rows are toggled in quick succession.
export const create = runtime.fn(api.create, { concurrent: true })
export const toggle = runtime.fn(api.toggle, { concurrent: true })
export const remove = runtime.fn(api.remove, { concurrent: true })
export const exportBytes = runtime.fn(api.exportBytes)
