// @vitest-environment happy-dom
import { Atom } from "@effect-atom/atom"
import { RegistryProvider } from "@effect-atom/atom-react"
import { cleanup, fireEvent, render, screen } from "@testing-library/react"
import { Effect, Layer, Stream } from "effect"
import { afterEach, describe, expect, it } from "vitest"
import { TodosError, Todo, TodoUpserted, type TodoId } from "../shared/todos-rpc"
import { App } from "./App"
import { runtime } from "./atoms"
import { TodosApi } from "./todos-api"

afterEach(cleanup)

const todo = new Todo({ id: "1" as TodoId, title: "Render me", completed: false, createdAt: 0 })

const failingCreateLayer = Layer.succeed(
  TodosApi,
  TodosApi.of({
    list: () => Effect.succeed([todo]),
    // create fails with a typed, expected domain error (e.g. write failed)
    create: () => Effect.fail(new TodosError({ reason: "WriteFailed", detail: "disk full" })),
    toggle: () => Effect.succeed(todo),
    remove: () => Effect.void,
    exportBytes: () => Effect.succeed(new Uint8Array()),
    changes: () => Stream.make(TodoUpserted.make({ todo }))
  })
)

describe("App create() failure handling", () => {
  it("does not leak an unhandled promise rejection when create() fails", async () => {
    const rejections: Array<unknown> = []
    const handler = (reason: unknown) => rejections.push(reason)
    process.on("unhandledRejection", handler)
    try {
      render(
        <RegistryProvider initialValues={[Atom.initialValue(runtime.layer, failingCreateLayer)]} scheduleTask={(f) => f()}>
          <App />
        </RegistryProvider>
      )

      const input = await screen.findByPlaceholderText("What needs doing?")
      fireEvent.change(input, { target: { value: "new todo" } })
      fireEvent.submit(input.closest("form")!)

      // flush microtasks + Node's rejection-tracking macrotask
      await new Promise((r) => setTimeout(r, 50))

      expect(rejections).toEqual([])
    } finally {
      process.off("unhandledRejection", handler)
    }
  })
})
