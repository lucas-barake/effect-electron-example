import { Effect } from "effect"
import { TodosRpcs } from "../shared/todos-rpc"
import { TodosRepo } from "./todos-repo"

export const TodosHandlersLive = TodosRpcs.toLayer(
  Effect.gen(function*() {
    const repo = yield* TodosRepo
    return TodosRpcs.of({
      TodosList: () => repo.list,
      TodoCreate: ({ title }) => repo.create(title),
      TodoToggle: ({ id }) => repo.toggle(id),
      TodoRemove: ({ id }) => repo.remove(id),
      TodosExport: () => repo.exportBytes,
      TodosWatch: () => repo.changes
    })
  })
)
