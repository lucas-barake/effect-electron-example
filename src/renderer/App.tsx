import { Result } from "@effect-atom/atom"
import { useAtomSet, useAtomValue } from "@effect-atom/atom-react"
import { Cause } from "effect"
import { type CSSProperties, type FormEvent, useState } from "react"
import * as atoms from "./atoms"

export function App() {
  const todos = useAtomValue(atoms.todos)
  const create = useAtomSet(atoms.create, { mode: "promise" })
  const toggle = useAtomSet(atoms.toggle)
  const remove = useAtomSet(atoms.remove)
  const exportBytes = useAtomSet(atoms.exportBytes, { mode: "promise" })
  const [title, setTitle] = useState("")

  const onSubmit = (event: FormEvent) => {
    event.preventDefault()
    const trimmed = title.trim()
    if (trimmed.length === 0) return
    setTitle("")
    // `mode: "promise"` rejects on a typed failure (atom-react flattenExit ->
    // Cause.squash). Without a catch the rejection escapes as an unhandled
    // promise rejection, so swallow it here (the UI has no mutation-error slot).
    create(trimmed).catch(() => {})
  }

  const onExport = () => {
    exportBytes()
      .then((bytes) => {
        const url = URL.createObjectURL(new Blob([new Uint8Array(bytes)], { type: "application/json" }))
        const anchor = document.createElement("a")
        anchor.href = url
        anchor.download = "todos.json"
        anchor.click()
        URL.revokeObjectURL(url)
      })
      .catch(() => {})
  }

  return (
    <main style={styles.main}>
      <header style={styles.header}>
        <h1 style={styles.title}>Todos</h1>
        <button type="button" style={styles.ghostButton} onClick={onExport}>Export</button>
      </header>

      <form style={styles.form} onSubmit={onSubmit}>
        <input
          style={styles.input}
          value={title}
          placeholder="What needs doing?"
          onChange={(event) => setTitle(event.target.value)}
        />
        <button type="submit" style={styles.button}>Add</button>
      </form>

      {Result.builder(todos)
        .onInitial(() => <p style={styles.muted}>Loading…</p>)
        .onFailure((cause) => <p style={styles.error}>{Cause.pretty(cause)}</p>)
        .onSuccess((items) =>
          items.length === 0
            ? <p style={styles.muted}>Nothing here yet.</p>
            : (
              <ul style={styles.list}>
                {items.map((todo) => (
                  <li key={todo.id} style={styles.item}>
                    <label style={styles.label}>
                      <input
                        type="checkbox"
                        checked={todo.completed}
                        onChange={() => toggle(todo.id)}
                      />
                      <span style={todo.completed ? styles.done : undefined}>{todo.title}</span>
                    </label>
                    <button type="button" style={styles.ghostButton} onClick={() => remove(todo.id)}>
                      Delete
                    </button>
                  </li>
                ))}
              </ul>
            )
        )
        .orNull()}
    </main>
  )
}

const styles = {
  main: { maxWidth: 560, margin: "0 auto", padding: 24, fontFamily: "system-ui, sans-serif" },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between" },
  title: { fontSize: 28, margin: 0 },
  form: { display: "flex", gap: 8, margin: "16px 0" },
  input: { flex: 1, padding: "8px 10px", fontSize: 15, border: "1px solid #ccc", borderRadius: 6 },
  button: { padding: "8px 16px", fontSize: 15, border: "none", borderRadius: 6, background: "#2563eb", color: "#fff", cursor: "pointer" },
  ghostButton: { padding: "4px 10px", fontSize: 13, border: "1px solid #ddd", borderRadius: 6, background: "transparent", cursor: "pointer" },
  list: { listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: 6 },
  item: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 10px", border: "1px solid #eee", borderRadius: 6 },
  label: { display: "flex", alignItems: "center", gap: 10 },
  done: { textDecoration: "line-through", color: "#999" },
  muted: { color: "#888" },
  error: { color: "#dc2626" }
} satisfies Record<string, CSSProperties>
