import { RegistryProvider } from "@effect-atom/atom-react"
import { createRoot } from "react-dom/client"
import { App } from "./App"

const container = document.getElementById("root")
if (container === null) throw new Error("Missing #root element")

createRoot(container).render(
  <RegistryProvider>
    <App />
  </RegistryProvider>
)
