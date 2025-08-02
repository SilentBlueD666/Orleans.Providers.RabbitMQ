using Newtonsoft.Json.Linq;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Streaming.RabbitMQ.Adapters;

[Serializable, GenerateSerializer, Immutable]
public sealed class RabbitMqBatchContainer : IBatchContainer
{
    [Id(0)]
    private EventSequenceTokenV2? _sequenceToken;

    [Id(1)]
    public StreamId StreamId { get; }

    [Id(2)]
    public IEnumerable<object> Events { get; }

    [Id(3)]
    public Dictionary<string, object> RequestContext { get; } = [];

    [Id(4)]
    public DateTime EnqueueTime { get; } = DateTime.UtcNow;

    public StreamSequenceToken SequenceToken => _sequenceToken 
        ?? throw new ArgumentNullException(nameof(_sequenceToken), "RealSequenceToken must be set before accessing SequenceToken.");

    internal EventSequenceTokenV2 RealSequenceToken
    {
        set { _sequenceToken = value; }
    }

    internal RabbitMqBatchContainer(
        EventSequenceTokenV2 sequenceToken,
        StreamId streamId,
        IEnumerable<object> events,
        Dictionary<string, object> requestContext,
        DateTime enqueueTime) : this(streamId, events, requestContext, enqueueTime)
    {
        _sequenceToken = sequenceToken ?? throw new ArgumentNullException(nameof(sequenceToken));
    }

    public RabbitMqBatchContainer(
        StreamId streamId,
        IEnumerable<object> events,
        Dictionary<string, object> requestContext,
        DateTime enqueueTime)
    {
        StreamId = streamId;
        Events = events ?? throw new ArgumentNullException(nameof(events));
        RequestContext = requestContext ?? throw new ArgumentNullException(nameof(requestContext));
        EnqueueTime = enqueueTime;
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() 
        => Events
            .OfType<T>()
            .Select((@event, index)
                => Tuple.Create<T, StreamSequenceToken>(
                    @event, 
                    _sequenceToken?.CreateSequenceTokenForEvent(index) 
                        ?? throw new InvalidOperationException("RealSequenceToken must be set before accessing GetEvents.") ));

    public bool ImportRequestContext()
    {
        if (RequestContext is null || RequestContext.Count == 0)
            return false;

        RequestContextExtensions.Import(RequestContext);
        return true;
    }
}