import { RpcSerialization } from "@effect/rpc"
import { describe, expect, it } from "@effect/vitest"
import { Effect, Equal, Schema } from "effect"
import { TodoEvent, TodoRemoved, TodosError, TodosRpcs, Todo, type TodoId, TodoUpserted } from "./todos-rpc"

describe("TodosRpcs contract", () => {
  it("exposes the expected RPC tags (inventory / drift guard)", () => {
    const tags = new Set(TodosRpcs.requests.keys())
    const expected = ["TodosList", "TodoCreate", "TodoToggle", "TodoRemove", "TodosExport", "TodosWatch"]
    for (const tag of expected) {
      expect(tags.has(tag)).toBe(true)
    }
    expect(tags.size).toBe(expected.length)
  })

  it("distinguishes TodosError values that differ only in detail (Equal/Hash)", () => {
    const a = new TodosError({ reason: "WriteFailed", detail: "one" })
    const b = new TodosError({ reason: "WriteFailed", detail: "two" })
    expect(Equal.equals(a, b)).toBe(false)
  })

  it.effect("MsgPack round-trips a native Uint8Array byte-equal (binary path)", () =>
    Effect.gen(function*() {
      const serialization = yield* RpcSerialization.RpcSerialization
      const parser = serialization.unsafeMake()
      const bytes = new Uint8Array([0, 1, 2, 254, 255])

      const encoded = parser.encode({ payload: bytes })
      expect(encoded).toBeInstanceOf(Uint8Array)

      const decoded = parser.decode(encoded as Uint8Array)
      expect(decoded).toHaveLength(1)
      const out = decoded[0] as { payload: Uint8Array }
      expect(out.payload).toBeInstanceOf(Uint8Array)
      expect(Array.from(out.payload)).toEqual([0, 1, 2, 254, 255])
    }).pipe(Effect.provide(RpcSerialization.layerMsgPack)))

  it.effect("round-trips a Todo through encode → MsgPack → decode", () =>
    Effect.gen(function*() {
      const serialization = yield* RpcSerialization.RpcSerialization
      const parser = serialization.unsafeMake()
      const todo = new Todo({ id: "abc" as TodoId, title: "Buy milk", completed: false, createdAt: 1000 })

      const wire = yield* Schema.encode(Todo)(todo)
      const packed = parser.encode(wire)
      const [unpacked] = parser.decode(packed as Uint8Array)
      const decoded = yield* Schema.decodeUnknown(Todo)(unpacked)

      expect(decoded).toStrictEqual(todo)
    }).pipe(Effect.provide(RpcSerialization.layerMsgPack)))

  it.effect("round-trips both TodoEvent variants (the TodosWatch payload) through MsgPack", () =>
    Effect.gen(function*() {
      const serialization = yield* RpcSerialization.RpcSerialization
      const todo = new Todo({ id: "abc" as TodoId, title: "Buy milk", completed: true, createdAt: 1000 })
      const events = [TodoUpserted.make({ todo }), TodoRemoved.make({ id: "abc" as TodoId })]

      for (const event of events) {
        const parser = serialization.unsafeMake()
        const wire = yield* Schema.encode(TodoEvent)(event)
        const [unpacked] = parser.decode(parser.encode(wire) as Uint8Array)
        const decoded = yield* Schema.decodeUnknown(TodoEvent)(unpacked)
        expect(decoded).toStrictEqual(event)
      }
    }).pipe(Effect.provide(RpcSerialization.layerMsgPack)))
})
