import { RpcServer } from "@effect/rpc"
import { app, BrowserWindow, MessageChannelMain, type MessagePortMain } from "electron"
import { Layer, ManagedRuntime } from "effect"
import { join } from "node:path"
import { TodosRpcs } from "../shared/todos-rpc"
import { type IpcServerPort, layerIpcServer, RpcPortHandoff } from "./ipc-server"
import { TodosHandlersLive } from "./todos-handlers"
import { TodosRepo } from "./todos-repo"

// The whole main-process backend: an RpcServer for the Todos contract, its
// handlers, the MessagePort transport, and the filesystem-backed repo (which
// resolves its own storage path). provideMerge keeps RpcPortHandoff in the
// output so the window code below can hand fresh ports to the running server.
const Live = RpcServer.layer(TodosRpcs, { disableFatalDefects: true }).pipe(
  Layer.provide(TodosHandlersLive),
  Layer.provideMerge(layerIpcServer),
  Layer.provide(TodosRepo.Default)
)

// ManagedRuntime keeps the layer scope open for the app's lifetime, so the
// forked RpcServer daemon stays alive across every renderer load (a plain
// Effect.runPromise would close the scope and tear the server down at once).
const runtime = ManagedRuntime.make(Live)

// Adapt Electron's EventEmitter-style MessagePortMain to the structural
// IpcServerPort the transport expects. The wrapped-listener map lets `off`
// detach the exact handler `on` registered, so a port swap leaves no leak.
const toServerPort = (port: MessagePortMain): IpcServerPort => {
  const wrapped = new Map<(...args: Array<any>) => void, (messageEvent: { data: unknown }) => void>()
  const on = ((event: "message" | "close", listener: (...args: Array<any>) => void) => {
    if (event === "message") {
      const handler = (messageEvent: { data: unknown }): void => listener({ data: messageEvent.data })
      wrapped.set(listener, handler)
      port.on("message", handler)
    } else {
      port.on("close", listener)
    }
  }) as IpcServerPort["on"]
  const off = ((_event: "message", listener: (...args: Array<any>) => void) => {
    const handler = wrapped.get(listener)
    if (handler !== undefined) {
      port.off("message", handler)
      wrapped.delete(listener)
    }
  }) as IpcServerPort["off"]
  return {
    on,
    off,
    postMessage: (message) => port.postMessage(message),
    start: () => port.start(),
    close: () => port.close()
  }
}

const createWindow = (bind: (port: IpcServerPort) => void): void => {
  const window = new BrowserWindow({
    width: 960,
    height: 720,
    webPreferences: {
      preload: join(__dirname, "../preload/index.js"),
      contextIsolation: true,
      sandbox: false
    }
  })

  // Mint a fresh MessageChannel per load: the renderer keeps port2, the server
  // binds port1. On reload this fires again, swapping ports — the transport
  // interrupts the previous load's in-flight streams.
  window.webContents.on("did-finish-load", () => {
    const channel = new MessageChannelMain()
    window.webContents.postMessage("rpc-port", null, [channel.port2])
    bind(toServerPort(channel.port1))
  })

  const devServerUrl = process.env.ELECTRON_RENDERER_URL
  if (devServerUrl !== undefined) {
    void window.loadURL(devServerUrl)
  } else {
    void window.loadFile(join(__dirname, "../renderer/index.html"))
  }
}

app.whenReady().then(
  () =>
    runtime.runPromise(RpcPortHandoff).then((handoff) => {
      createWindow(handoff.bind)
      app.on("activate", () => {
        if (BrowserWindow.getAllWindows().length === 0) createWindow(handoff.bind)
      })
    }),
  (cause) => {
    console.error("Failed to start the Effect RPC server", cause)
    app.quit()
  }
)

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") {
    void runtime.dispose().finally(() => app.quit())
  }
})
