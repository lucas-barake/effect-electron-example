import type { RpcClientError } from "@effect/rpc/RpcClientError"
import { Context, type Effect, type Stream } from "effect"
import { type TodoEvent, type TodoNotFound, type TodosError, type Todo, type TodoId } from "../shared/todos-rpc"

// `TodosApi` is the renderer-facing service the atoms consume. It is a
// Context.Tag so atoms read `yield* TodosApi` and tests swap in a mock with
// `Layer.succeed(TodosApi, …)` (via the atom runtime's layer override) instead
// of mocking the RPC client's inferred types. In production it is backed by the
// @effect/rpc client over the MessagePort transport; atoms never touch it.
export class TodosApi extends Context.Tag("renderer/TodosApi")<
  TodosApi,
  {
    readonly list: () => Effect.Effect<ReadonlyArray<Todo>, RpcClientError>
    readonly create: (title: string) => Effect.Effect<Todo, TodosError | RpcClientError>
    readonly toggle: (id: TodoId) => Effect.Effect<Todo, TodoNotFound | TodosError | RpcClientError>
    readonly remove: (id: TodoId) => Effect.Effect<void, TodoNotFound | TodosError | RpcClientError>
    readonly exportBytes: () => Effect.Effect<Uint8Array, TodosError | RpcClientError>
    readonly changes: () => Stream.Stream<TodoEvent, RpcClientError>
  }
>() {}
