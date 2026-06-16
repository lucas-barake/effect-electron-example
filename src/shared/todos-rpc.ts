import { Rpc, RpcGroup } from "@effect/rpc"
import { Schema } from "effect"

export const TodoId = Schema.String.pipe(Schema.brand("TodoId"))
export type TodoId = typeof TodoId.Type

export const TodoTitle = Schema.NonEmptyTrimmedString
export type TodoTitle = typeof TodoTitle.Type

export class Todo extends Schema.Class<Todo>("Todo")({
  id: TodoId,
  title: TodoTitle,
  completed: Schema.Boolean,
  createdAt: Schema.Number
}) {}

// Wire-level tagged errors shared by the renderer (client) and main (server).
// No `cause` field: a JS Error cause never survives the MsgPack boundary, so the
// contract only exposes serializable, typed failure data.
export class TodoNotFound extends Schema.TaggedError<TodoNotFound>()("TodoNotFound", {
  id: TodoId
}) {}

// `detail` (not `message`): a `message` field on a Schema.TaggedError shadows the
// non-enumerable Error.prototype.message, which hides it from Equal/Hash and makes
// the atom registry dedupe distinct failures (showing a stale error).
export class TodosError extends Schema.TaggedError<TodosError>()("TodosError", {
  reason: Schema.Literal("ReadFailed", "WriteFailed", "Corrupted"),
  detail: Schema.String
}) {}

// The change events TodosWatch streams: one per mutation. Upserted covers create
// and toggle (the renderer replaces by id); Removed carries only the id, since a
// deleted todo has no body left to send.
export const TodoUpserted = Schema.TaggedStruct("TodoUpserted", { todo: Todo })
export const TodoRemoved = Schema.TaggedStruct("TodoRemoved", { id: TodoId })
export const TodoEvent = Schema.Union(TodoUpserted, TodoRemoved)
export type TodoEvent = typeof TodoEvent.Type

export class TodosRpcs extends RpcGroup.make(
  Rpc.make("TodosList", {
    success: Schema.Array(Todo)
  }),
  Rpc.make("TodoCreate", {
    payload: { title: TodoTitle },
    success: Todo,
    error: TodosError
  }),
  Rpc.make("TodoToggle", {
    payload: { id: TodoId },
    success: Todo,
    error: Schema.Union(TodoNotFound, TodosError)
  }),
  Rpc.make("TodoRemove", {
    payload: { id: TodoId },
    error: Schema.Union(TodoNotFound, TodosError)
  }),
  // Binary path: the persisted JSON file is shipped as a native Uint8Array
  // (MsgPack-copied, never base64) — the renderer can save it to disk.
  Rpc.make("TodosExport", {
    success: Schema.Uint8ArrayFromSelf,
    error: TodosError
  }),
  // Server-stream of individual change events (upsert/remove), one per mutation;
  // the renderer folds them into a live list — no polling or manual refetching.
  Rpc.make("TodosWatch", {
    success: TodoEvent,
    stream: true
  })
) {}
