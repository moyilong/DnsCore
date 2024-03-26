using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace DnsCore.Client.Transport;

internal abstract class DnsClientSocketTransport : DnsClientTransport
{
    private readonly EndPoint _remoteEndPoint;

    protected Socket Socket { get; }

    protected DnsClientSocketTransport(EndPoint remoteEndPoint, SocketType socketType, ProtocolType protocolType)
    {
        _remoteEndPoint = remoteEndPoint;
        Socket = new(remoteEndPoint.AddressFamily, socketType, protocolType);
        Socket.Bind(new IPEndPoint(remoteEndPoint.AddressFamily == AddressFamily.InterNetwork ? IPAddress.Any : IPAddress.IPv6Any, 0));
    }

    protected async ValueTask EnsureConnected(CancellationToken cancellationToken)
    {
        if (Socket.Connected)
            return;

        try
        {
            await Socket.ConnectAsync(_remoteEndPoint, cancellationToken).ConfigureAwait(false);
        }
        catch (SocketException e)
        {
            throw new DnsClientTransportException("Failed to connect to server", e);
        }
    }

    public override ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        try
        {
            Socket.Dispose();
            return ValueTask.CompletedTask;
        }
        catch (SocketException e)
        {
            return ValueTask.FromException(e);
        }
    }
}