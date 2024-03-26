using System;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using DnsCore.Common;

namespace DnsCore.Client.Transport;

internal sealed class DnsClientTcpTransport(EndPoint remoteEndPoint) : DnsClientSocketTransport(remoteEndPoint, SocketType.Stream, ProtocolType.Tcp)
{
    private readonly Channel<DnsTransportMessage> _receiveChannel = Channel.CreateUnbounded<DnsTransportMessage>();

    public override ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public override async ValueTask Send(DnsTransportMessage requestMessage, CancellationToken cancellationToken)
    {
        var lengthBuffer = DnsBufferPool.Rent(2);
        var lengthBufferMem = lengthBuffer.AsMemory(0, 2);
        using var socket = CreateSocket();
        socket.NoDelay = true;
        try
        {
            await socket.ConnectAsync(RemoteEndPoint, cancellationToken).ConfigureAwait(false);

            // Sending
            var buffer = requestMessage.Buffer;
            BinaryPrimitives.WriteUInt16BigEndian(lengthBufferMem.Span, (ushort)buffer.Length);
            await socket.SendAsync(lengthBufferMem, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            while (!buffer.IsEmpty)
            {
                var sentBytes = await socket.SendAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
                buffer = buffer[sentBytes..];
            }

            // Receiving
            var receivedBytes = await socket.ReceiveAsync(lengthBufferMem, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            if (receivedBytes == 0)
                throw new DnsClientTransportException("Failed to receive response");

            var length = BinaryPrimitives.ReadUInt16BigEndian(lengthBufferMem.Span);
            if (length == 0)
                throw new DnsClientTransportException("Failed to receive response");

            var responseBuffer = DnsBufferPool.Rent(length);
            var totalReceivedBytes = 0;
            while (totalReceivedBytes < length)
            {
                receivedBytes = await socket.ReceiveAsync(responseBuffer.AsMemory(totalReceivedBytes, length - totalReceivedBytes), SocketFlags.None, cancellationToken).ConfigureAwait(false);
                if (receivedBytes == 0)
                    throw new DnsClientTransportException("Failed to receive response");
                totalReceivedBytes += receivedBytes;
            }

            await _receiveChannel.Writer.WriteAsync(new DnsTransportMessage(responseBuffer, totalReceivedBytes), cancellationToken).ConfigureAwait(false);
        }
        catch (SocketException e)
        {
            throw new DnsClientTransportException("Failed to send request", e);
        }
        finally
        {
            DnsBufferPool.Return(lengthBuffer);
        }
    }

    public override async ValueTask<DnsTransportMessage> Receive(CancellationToken cancellationToken)
    {
        return await _receiveChannel.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
    }
}