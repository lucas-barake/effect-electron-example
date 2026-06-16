import { RpcSerialization, RpcServer } from "@effect/rpc"
import type { FromClientEncoded, FromServerEncoded } from "@effect/rpc/RpcMessage"
import { Context, Effect, Layer, Mailbox, Stream } from "effect"

export interface IpcServerPort {
  on(event: "message", listener: (event: { data: string | Uint8Array }) => void): void
  on(event: "close", listener: () => void): void
  off(event: "message", listener: (event: { data: string | Uint8Array }) => void): void
  postMessage(message: string | Uint8Array): void
  start(): void
  close(): void
}

// Exposes `bind` so the Electron bootstrap can hand the long-lived server a fresh
// port on every window (re)load. It's a Context.Tag because a layer publishes its
// values as services — index.ts reads `bind` back via runtime.runPromise(RpcPortHandoff).
export class RpcPortHandoff extends Context.Tag("effect-electron-example/RpcPortHandoff")<
  RpcPortHandoff,
  { readonly bind: (port: IpcServerPort) => void }
>() {}

interface ActivePort {
  readonly port: IpcServerPort
  readonly id: number
  readonly parser: RpcSerialization.Parser
  readonly handler: (event: { data: string | Uint8Array }) => void
}

export const layerIpcServer: Layer.Layer<RpcServer.Protocol | RpcPortHandoff> = Layer.unwrapScoped(
  Effect.gen(function*() {
    // The one shared channel: `bind` (producer) hands ports in here, the protocol's
    // drain (consumer) takes them out. Built once and closed over by both layers.
    const portInbox = yield* Mailbox.make<IpcServerPort>()

    const protocol = Layer.scoped(
      RpcServer.Protocol,
      RpcServer.Protocol.make(Effect.fnUntraced(function*(writeRequest) {
        const serialization = yield* RpcSerialization.RpcSerialization
        const disconnects = yield* Mailbox.make<number>()
        const inbound = yield* Mailbox.make<readonly [number, FromClientEncoded]>()
        let nextClientId = 0
        let current: ActivePort | null = null

        const bindPort = (newPort: IpcServerPort): void => {
          if (current) {
            current.port.off("message", current.handler)
            current.port.close()
            // Offer the replaced client to disconnects so RpcServer interrupts
            // its in-flight fibers (streams included) on renderer reload.
            disconnects.unsafeOffer(current.id)
          }
          const id = nextClientId++
          const parser = serialization.unsafeMake()
          const handler = (event: { data: string | Uint8Array }): void => {
            try {
              for (const message of parser.decode(event.data)) {
                inbound.unsafeOffer([id, message as FromClientEncoded])
              }
            } catch {
              // Drop a malformed frame rather than throwing inside the host's
              // raw message callback (one bad message shouldn't take down the connection).
            }
          }
          newPort.on("message", handler)
          newPort.on("close", () => disconnects.unsafeOffer(id))
          newPort.start()
          current = { port: newPort, id, parser, handler }
        }

        // Drain inbound frames and bind handed-off ports on scoped fibers, so
        // writeRequest/bindPort never run inside a raw port callback.
        yield* Effect.forkScoped(
          Mailbox.toStream(inbound).pipe(
            // Drop frames from a port that was swapped out (renderer reload): the
            // server already saw that client's disconnect, so forwarding a late
            // frame would recreate it as a zombie client whose fibers never end.
            Stream.runForEach(([id, message]) => id === current?.id ? writeRequest(id, message) : Effect.void)
          )
        )
        yield* Effect.forkScoped(
          Mailbox.toStream(portInbox).pipe(
            Stream.runForEach((port) => Effect.sync(() => bindPort(port)))
          )
        )
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            if (current) {
              current.port.off("message", current.handler)
              current.port.close()
            }
          })
        )

        const send = (clientId: number, response: FromServerEncoded): Effect.Effect<void> =>
          Effect.sync(() => {
            if (current?.id === clientId) {
              const encoded = current.parser.encode(response)
              if (encoded !== undefined) {
                current.port.postMessage(encoded)
              }
            }
          })

        return {
          disconnects,
          send,
          end: () => Effect.void,
          clientIds: Effect.sync(() => new Set(current ? [current.id] : [])),
          initialMessage: Effect.succeedNone,
          supportsAck: true,
          // See the client transport: Electron's MessagePortMain cannot transfer
          // ArrayBuffers (electron#34905), so binary rides as MsgPack-copied bytes.
          supportsTransferables: false,
          supportsSpanPropagation: false
        }
      })
    ))

    const handoff = Layer.succeed(RpcPortHandoff, {
      bind: (port: IpcServerPort): void => {
        portInbox.unsafeOffer(port)
      }
    })

    return Layer.merge(protocol, handoff)
  })
).pipe(Layer.provide(RpcSerialization.layerMsgPack))
