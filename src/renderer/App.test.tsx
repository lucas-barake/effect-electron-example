// @vitest-environment happy-dom
import { Atom } from "@effect-atom/atom"
import { RegistryProvider } from "@effect-atom/atom-react"
import { RpcClientError } from "@effect/rpc/RpcClientError"
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { Effect, Layer, Stream } from "effect"
import { afterEach, describe, expect, it, vi } from "vitest"
import { Todo, type TodoEvent, TodoUpserted, type TodoId } from "../shared/todos-rpc"
import { App } from "./App"
import { runtime } from "./atoms"
import { TodosApi } from "./todos-api"

afterEach(cleanup)

const todo = new Todo({ id: "1" as TodoId, title: "Render me", completed: false, createdAt: 0 })

const makeMock = (changes: () => Stream.Stream<TodoEvent, RpcClientError>) => {
  const calls: Array<{ method: string; args: unknown }> = []
  const layer = Layer.succeed(
    TodosApi,
    TodosApi.of({
      list: () => Effect.succeed([todo]),
      create: (title) => {
        calls.push({ method: "create", args: title })
        return Effect.succeed(todo)
      },
      toggle: (id) => {
        calls.push({ method: "toggle", args: id })
        return Effect.succeed(todo)
      },
      remove: (id) => {
        calls.push({ method: "remove", args: id })
        return Effect.void
      },
      exportBytes: () => Effect.succeed(new Uint8Array()),
      changes
    })
  )
  return { calls, layer }
}

const renderApp = (changes: () => Stream.Stream<TodoEvent, RpcClientError>) => {
  const { calls, layer } = makeMock(changes)
  render(
    <RegistryProvider initialValues={[Atom.initialValue(runtime.layer, layer)]} scheduleTask={(f) => f()}>
      <App />
    </RegistryProvider>
  )
  return calls
}

describe("App", () => {
  it("renders todos streamed from the TodosApi", async () => {
    renderApp(() => Stream.make(TodoUpserted.make({ todo })))
    expect(await screen.findByText("Render me")).toBeTruthy()
  })

  it("renders the empty state when there are no todos", async () => {
    renderApp(() => Stream.empty)
    expect(await screen.findByText("Nothing here yet.")).toBeTruthy()
  })

  it("renders the failure cause when the changes stream fails", async () => {
    renderApp(() => Stream.fail(new RpcClientError({ reason: "Protocol", message: "stream boom" })))
    expect(await screen.findByText(/stream boom|RpcClientError/)).toBeTruthy()
  })

  it("submitting a title calls create with the trimmed value and clears the input", async () => {
    const calls = renderApp(() => Stream.make(TodoUpserted.make({ todo })))
    const input = await screen.findByPlaceholderText("What needs doing?")
    fireEvent.change(input, { target: { value: "  new todo  " } })
    fireEvent.submit(input.closest("form")!)

    await waitFor(() => expect(calls).toContainEqual({ method: "create", args: "new todo" }))
    expect((input as HTMLInputElement).value).toBe("")
  })

  it("a blank submit does not call create", async () => {
    const calls = renderApp(() => Stream.make(TodoUpserted.make({ todo })))
    const input = await screen.findByPlaceholderText("What needs doing?")
    fireEvent.change(input, { target: { value: "   " } })
    fireEvent.submit(input.closest("form")!)

    expect(calls.some((call) => call.method === "create")).toBe(false)
  })

  it("toggling and deleting invoke the api with the todo id", async () => {
    const calls = renderApp(() => Stream.make(TodoUpserted.make({ todo })))
    await screen.findByText("Render me")
    fireEvent.click(screen.getByRole("checkbox"))
    fireEvent.click(screen.getByText("Delete"))

    await waitFor(() => expect(calls).toContainEqual({ method: "toggle", args: "1" }))
    await waitFor(() => expect(calls).toContainEqual({ method: "remove", args: "1" }))
  })

  it("exporting downloads the bytes as todos.json", async () => {
    const createObjectURL = vi.fn((_blob: Blob) => "blob:mock")
    const revokeObjectURL = vi.fn()
    const click = vi.fn()
    const original = {
      create: URL.createObjectURL,
      revoke: URL.revokeObjectURL,
      click: HTMLAnchorElement.prototype.click
    }
    URL.createObjectURL = createObjectURL as typeof URL.createObjectURL
    URL.revokeObjectURL = revokeObjectURL as typeof URL.revokeObjectURL
    HTMLAnchorElement.prototype.click = click

    try {
      renderApp(() => Stream.make(TodoUpserted.make({ todo })))
      fireEvent.click(await screen.findByText("Export"))

      await waitFor(() => expect(click).toHaveBeenCalled())
      expect(createObjectURL.mock.calls[0]![0].type).toBe("application/json")
      expect((click.mock.instances[0] as HTMLAnchorElement).download).toBe("todos.json")
      expect(revokeObjectURL).toHaveBeenCalledWith("blob:mock")
    } finally {
      URL.createObjectURL = original.create
      URL.revokeObjectURL = original.revoke
      HTMLAnchorElement.prototype.click = original.click
    }
  })
})
