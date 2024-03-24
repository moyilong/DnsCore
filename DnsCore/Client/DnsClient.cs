using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using DnsCore.Internal;
using DnsCore.Model;

namespace DnsCore.Client;

public sealed class DnsClient : IDisposable, IAsyncDisposable
{
    private readonly EndPoint _serverEndPoint;
    private readonly TimeSpan _requestTimeout;
    private readonly Socket _socket;
    private readonly Dictionary<ushort, TaskCompletionSource<DnsResponse>> _pendingRequests = [];
    private readonly ReaderWriterLockSlim _pendingRequestsLock = new();
    private readonly CancellationTokenSource _receiveTaskCancellation = new();
    private readonly Task _receiveTask;

    public DnsClient(EndPoint serverEndPoint, TimeSpan requestTimeout)
    {
        _serverEndPoint = serverEndPoint;
        _requestTimeout = requestTimeout;
        _socket = new(serverEndPoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
        _socket.Bind(new IPEndPoint(IPAddress.Any, 0));
        _receiveTask = ReceiveResponses(_receiveTaskCancellation.Token);
    }

    public DnsClient(EndPoint serverEndPoint) : this(serverEndPoint, TimeSpan.FromSeconds(5)) {}
    public DnsClient(IPAddress serverAddress, ushort port, TimeSpan requestTimeout) : this(new IPEndPoint(serverAddress, port), requestTimeout) {}
    public DnsClient(IPAddress serverAddress, ushort port) : this(new IPEndPoint(serverAddress, port)) {}
    public DnsClient(IPAddress serverAddress, TimeSpan requestTimeout) : this(serverAddress, DnsDefaults.Port, requestTimeout) {}
    public DnsClient(IPAddress serverAddress) : this(serverAddress, DnsDefaults.Port) {}

    public void Dispose()
    {
        _receiveTaskCancellation.Cancel();
        _receiveTask.Wait();
        _receiveTaskCancellation.Dispose();
        _socket.Dispose();
        _pendingRequestsLock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        await _receiveTaskCancellation.CancelAsync();
        await _receiveTask;
        _receiveTaskCancellation.Dispose();
        _socket.Dispose();
        _pendingRequestsLock.Dispose();
    }

    public async ValueTask<DnsResponse> Query(DnsRequest request, CancellationToken cancellationToken = default)
    {
        var responseCompletion = AddRequest(request.Id);
        using var timeoutCancellation = new CancellationTokenSource(_requestTimeout);
        using var aggregatedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellation.Token);
        await using var cancellationRegistration = aggregatedCancellation.Token.Register(() => responseCompletion.TrySetCanceled()).ConfigureAwait(false);
        try
        {
            await SendRequest(request, aggregatedCancellation.Token).ConfigureAwait(false);
            return await responseCompletion.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException e) when (e.CancellationToken == timeoutCancellation.Token)
        {
            throw new TimeoutException("DNS request timed out");
        }
        finally
        {
            RemoveRequest(request.Id, responseCompletion);
        }
    }

    public async ValueTask<DnsResponse> Query(DnsName name, DnsRecordType type, CancellationToken cancellationToken = default)
    {
        return await Query(new DnsRequest(name, type), cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask SendRequest(DnsRequest request, CancellationToken cancellationToken)
    {
        var buffer = DnsBufferPool.Rent(DnsDefaults.MaxUdpMessageSize);
        try
        {
            var len = request.Encode(buffer);
            await _socket.SendToAsync(buffer.AsMemory(0, len), SocketFlags.None, _serverEndPoint, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DnsBufferPool.Return(buffer);
        }
    }

    private async Task ReceiveResponses(CancellationToken cancellationToken)
    {
        await Task.Yield();
        var buffer = DnsBufferPool.Rent(DnsDefaults.MaxUdpMessageSize);
        try
        {
            while (true)
            {
                var result = await _socket.ReceiveFromAsync(buffer, SocketFlags.None, _serverEndPoint, cancellationToken).ConfigureAwait(false);
                CompleteRequest(DnsResponse.Decode(buffer.AsSpan(0, result.ReceivedBytes)));
            }
        }
        finally
        {
            DnsBufferPool.Return(buffer);
        }
    }

    private TaskCompletionSource<DnsResponse> AddRequest(ushort requestId)
    {
        _pendingRequestsLock.EnterWriteLock();
        try
        {
            var completion = new TaskCompletionSource<DnsResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[requestId] = completion;
            return completion;
        }
        finally
        {
            _pendingRequestsLock.ExitWriteLock();
        }
    }

    private void RemoveRequest(ushort requestId, TaskCompletionSource<DnsResponse> completion)
    {
        _pendingRequestsLock.EnterWriteLock();
        try
        {
            if (_pendingRequests.TryGetValue(requestId, out var current) && current == completion)
                _pendingRequests.Remove(requestId);
        }
        finally
        {
            _pendingRequestsLock.ExitWriteLock();
        }
    }

    private void CompleteRequest(DnsResponse response)
    {
        _pendingRequestsLock.EnterReadLock();
        try
        {
            if (_pendingRequests.TryGetValue(response.Id, out var completion))
                completion.TrySetResult(response);
        }
        finally
        {
            _pendingRequestsLock.ExitReadLock();
        }
    }
}