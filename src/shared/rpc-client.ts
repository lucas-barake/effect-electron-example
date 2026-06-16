import { RpcClient, RpcSerialization } from "@effect/rpc"
import type { FromClientEncoded, FromServerEncoded } from "@effect/rpc/RpcMessage"
import { Effect, Layer, Mailbox, Stream } from "effect"

export interface IpcClientPort {
  onmessage: ((event: { data: string | Uint8Array }) => void) | null
  postMessage: (message: string | Uint8Array) => void
  start: () => void
  close: () => void
}

export const makeIpcClientProtocol = (port: IpcClientPort) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function*(writeResponse) {
      const serialization = yield* RpcSerialization.RpcSerialization
      const parser = serialization.unsafeMake()
      const inbound = yield* Mailbox.make<FromServerEncoded>()

      port.onmessage = (event) => {
        try {
          for (const decoded of parser.decode(event.data)) {
            inbound.unsafeOffer(decoded as FromServerEncoded)
          }
        } catch {
          // Drop a malformed frame rather than throwing inside the host's raw
          // message callback (one bad message shouldn't take down the connection).
        }
      }
      // Detach and close on scope teardown so a rebuilt client (renderer reload,
      // fresh port) leaves no stale closure writing into a dead registry.
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          port.onmessage = null
          port.close()
        })
      )
      port.start()

      // Order inbound frames through a Mailbox drained on a scoped fiber, so
      // writeResponse never runs inside the raw onmessage callback.
      yield* Effect.forkScoped(
        Mailbox.toStream(inbound).pipe(Stream.runForEach(writeResponse))
      )

      const send = (request: FromClientEncoded) =>
        Effect.sync(() => {
          const encoded = parser.encode(request)
          if (encoded !== undefined) {
            port.postMessage(encoded)
          }
        })

      return {
        send,
        supportsAck: true,
        // Electron's MessagePortMain cannot transfer ArrayBuffers (electron#34905);
        // binary rides as MsgPack-copied bytes instead of zero-copy transfer.
        supportsTransferables: false
      }
    })
  )

export const layerIpcClient = (port: IpcClientPort): Layer.Layer<RpcClient.Protocol> =>
  Layer.scoped(RpcClient.Protocol, makeIpcClientProtocol(port)).pipe(
    Layer.provide(RpcSerialization.layerMsgPack)
  )
