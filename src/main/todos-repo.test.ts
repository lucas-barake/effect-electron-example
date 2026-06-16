import { mkdirSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { FileSystem } from "@effect/platform"
import { NodeContext } from "@effect/platform-node"
import { describe, expect, it } from "@effect/vitest"
import { Chunk, Deferred, Effect, Fiber, type Scope, Stream } from "effect"
import { afterEach, beforeEach, vi } from "vitest"
import { Todo, TodoRemoved, type TodoId, TodoUpserted } from "../shared/todos-rpc"
import { TodosRepo } from "./todos-repo"

// TodosRepo hard-codes its file under Electron's userData dir. Point app.getPath
// at a fresh temp dir per test so each case is isolated and the disk writes real.
// (vi.mock is hoisted above the import above, so the repo loads this fake electron.)
const electron = vi.hoisted(() => ({ userDataDir: "" }))
vi.mock("electron", () => ({ app: { getPath: () => electron.userDataDir } }))

let filePath: string
beforeEach(() => {
  electron.userDataDir = mkdtempSync(join(tmpdir(), "todos-"))
  filePath = join(electron.userDataDir, "todos.json")
})
afterEach(() => {
  rmSync(electron.userDataDir, { recursive: true, force: true })
})

const makeRepo = Effect.provide(TodosRepo, TodosRepo.Default)

const run = <A, E>(self: Effect.Effect<A, E, FileSystem.FileSystem | Scope.Scope>) =>
  self.pipe(Effect.provide(NodeContext.layer), Effect.scoped)

describe("TodosRepo (filesystem-backed)", () => {
  it.effect("creates a todo and lists it back", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const created = yield* repo.create("Buy milk")
      expect(created.title).toBe("Buy milk")
      expect(created.completed).toBe(false)
      expect(yield* repo.list).toStrictEqual([created])
    })))

  it.effect("create writes the todos file to disk", () =>
    run(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const repo = yield* makeRepo
      yield* repo.create("On disk")
      expect(yield* fs.readFileString(filePath)).toContain("On disk")
    })))

  it.effect("toggles a todo's completed flag", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const created = yield* repo.create("Task")
      const toggled = yield* repo.toggle(created.id)
      expect(toggled.completed).toBe(true)
      expect(toggled.id).toBe(created.id)
      const list = yield* repo.list
      expect(list[0]?.completed).toBe(true)
    })))

  it.effect("toggling one todo in a multi-todo list leaves the others unchanged", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const first = yield* repo.create("First")
      const second = yield* repo.create("Second")

      yield* repo.toggle(second.id)
      const list = yield* repo.list

      const toggledSecond = new Todo({
        id: second.id,
        title: second.title,
        completed: true,
        createdAt: second.createdAt
      })
      expect(list).toStrictEqual([first, toggledSecond])
    })))

  it.effect("toggle fails with TodoNotFound for an unknown id", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const error = yield* Effect.flip(repo.toggle("missing" as TodoId))
      expect(error._tag).toBe("TodoNotFound")
    })))

  it.effect("removes a todo", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const created = yield* repo.create("Delete me")
      yield* repo.remove(created.id)
      expect(yield* repo.list).toStrictEqual([])
    })))

  it.effect("remove fails with TodoNotFound for an unknown id", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const error = yield* Effect.flip(repo.remove("missing" as TodoId))
      expect(error._tag).toBe("TodoNotFound")
    })))

  it.effect("persists todos across a fresh repo instance (reload)", () =>
    run(Effect.gen(function*() {
      const repo1 = yield* makeRepo
      const created = yield* repo1.create("Persisted")
      const repo2 = yield* makeRepo
      expect(yield* repo2.list).toStrictEqual([created])
    })))

  it.effect("loads a pre-existing valid todos.json written out-of-band at init", () =>
    run(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      const seeded = [{ id: "seed-1", title: "Seeded", completed: true, createdAt: 5 }]
      yield* fs.writeFileString(filePath, JSON.stringify(seeded))

      const repo = yield* makeRepo
      const list = yield* repo.list

      expect(list).toHaveLength(1)
      expect(list[0]).toBeInstanceOf(Todo)
      expect(list[0]?.title).toBe("Seeded")
      expect(list[0]?.completed).toBe(true)
      expect(list[0]?.id).toBe("seed-1")
    })))

  it.effect("fails init with a Corrupted TodosError when the stored file is invalid", () =>
    run(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      yield* fs.writeFileString(filePath, "{ not valid json")

      // Init runs during layer construction, so the failure surfaces when the
      // repo layer is built.
      const error = yield* Effect.flip(makeRepo)

      expect(error._tag).toBe("TodosError")
      expect(error.reason).toBe("Corrupted")
    })))

  it.effect("fails init with Corrupted when a stored todo has a blank/untrimmed title", () =>
    run(Effect.gen(function*() {
      const fs = yield* FileSystem.FileSystem
      yield* fs.writeFileString(
        filePath,
        JSON.stringify([{ id: "seed-1", title: "   ", completed: false, createdAt: 1 }])
      )

      const error = yield* Effect.flip(makeRepo)

      expect(error._tag).toBe("TodosError")
      expect(error.reason).toBe("Corrupted")
    })))

  it.effect("fails init with a ReadFailed TodosError when the stored file cannot be read", () =>
    run(Effect.gen(function*() {
      // Put a directory at the file path so init's readFileString fails with
      // EISDIR — not NotFound (which is caught) and not a ParseError — exercising
      // the ReadFailed mapping.
      yield* Effect.sync(() => mkdirSync(filePath))

      const error = yield* Effect.flip(makeRepo)

      expect(error._tag).toBe("TodosError")
      expect(error.reason).toBe("ReadFailed")
    })))

  it.effect("surfaces a WriteFailed TodosError when the file cannot be written", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      // Put a directory where the file should be, so writeFileString fails with
      // EISDIR regardless of process privileges (a chmod-based test would not
      // hold under a root CI).
      yield* Effect.sync(() => mkdirSync(filePath))

      const error = yield* Effect.flip(repo.create("nope"))

      expect(error._tag).toBe("TodosError")
      expect(error.reason).toBe("WriteFailed")
    })))

  it.effect("exportBytes returns an empty array before anything exists", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const bytes = yield* repo.exportBytes
      expect(new TextDecoder().decode(bytes)).toBe("[]")
    })))

  it.effect("exportBytes returns the persisted JSON bytes after a create", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      yield* repo.create("Exported")
      const bytes = yield* repo.exportBytes
      expect(bytes).toBeInstanceOf(Uint8Array)
      expect(new TextDecoder().decode(bytes)).toContain("Exported")
    })))

  it.effect("streams an upsert for new todos and a removal when one is deleted", () =>
    run(Effect.gen(function*() {
      const repo = yield* makeRepo
      const seed = yield* repo.create("Seed")

      // The Deferred resolves on the first emission — the initial snapshot replayed
      // as an upsert — proving the subscription is live before we mutate.
      const subscribed = yield* Deferred.make<void>()
      const fiber = yield* repo.changes.pipe(
        Stream.tap(() => Deferred.succeed(subscribed, undefined)),
        Stream.take(3),
        Stream.runCollect,
        Effect.fork
      )
      yield* Deferred.await(subscribed)

      const created = yield* repo.create("Live")
      yield* repo.remove(created.id)
      const events = Chunk.toReadonlyArray(yield* Fiber.join(fiber))

      expect(events).toStrictEqual([
        TodoUpserted.make({ todo: seed }),
        TodoUpserted.make({ todo: created }),
        TodoRemoved.make({ id: created.id })
      ])
    })))
})
