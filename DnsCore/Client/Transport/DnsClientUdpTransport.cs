using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using DnsCore.Common;

namespace DnsCore.Client.Transport;

internal sealed class DnsClientUdpTransport(EndPoint remoteEndPoint) : DnsClientSocketTransport(remoteEndPoint, SocketType.Dgram, ProtocolType.Udp)
{
    public override async ValueTask Send(DnsTransportMessage requestMessage, CancellationToken cancellationToken)
    {
        await EnsureConnected(cancellationToken);
        try
        {
            await Socket.SendAsync(requestMessage.Buffer, SocketFlags.None, cancellationToken);
        }
        catch (SocketException e)
        {
            throw new DnsClientTransportException("Failed to send request", e);
        }
    }

    public override async ValueTask<DnsTransportMessage> Receive(CancellationToken cancellationToken)
    {
        await EnsureConnected(cancellationToken);
        var buffer = DnsBufferPool.Rent(DnsDefaults.MaxUdpMessageSize);
        try
        {
            var receivedBytes = await Socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
            return new DnsTransportMessage(buffer, receivedBytes);
        }
        catch (SocketException e)
        {
            throw new DnsClientTransportException("Failed to receive response", e);
        }
    }
}