using Orleans.Configuration;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Streaming.RabbitMQ;

internal static class RabbitMqChannelExtensions
{
    /// <summary>Asynchronously acknowledges one message.</summary>
    /// <param name="deliveryTag">The delivery tag.</param>
    /// <param name="cancellationToken">Cancellation token for this operation.</param>
    public static async ValueTask BasicAckAsync(this IChannel channel, ulong deliveryTag, CancellationToken cancellationToken = default)
        => await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Rejects a message from the queue asynchronously, indicating that the message will not be processed.
    /// </summary>
    /// <remarks>This method rejects the specified message without requeuing it, meaning the message will be
    /// discarded. Use this method when the message cannot or should not be processed and should not be
    /// redelivered.</remarks>
    /// <param name="channel">The channel through which the message rejection is performed. Cannot be <see langword="null"/>.</param>
    /// <param name="deliveryTag">The delivery tag of the message to reject. This tag uniquely identifies the message within the channel.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
    public static async ValueTask BasicRejectAsync(this IChannel channel, ulong deliveryTag, CancellationToken cancellationToken = default)
        => await channel.BasicRejectAsync(deliveryTag, requeue: false, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Rejects a message from the queue asynchronously and requests that it be requeued for future processing.
    /// </summary>
    /// <param name="channel">The channel through which the message rejection is performed. Cannot be <see langword="null"/>.</param>
    /// <param name="deliveryTag">The delivery tag of the message to reject. This tag uniquely identifies the message within the channel.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests. The default value is <see cref="CancellationToken.None"/>.</param>
    public static async ValueTask BasicRequeueAsync(this IChannel channel, ulong deliveryTag, CancellationToken cancellationToken = default)
        => await channel.BasicRejectAsync(deliveryTag, requeue: true, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Declares an exchange.
    /// </summary>
    /// <param name="channel">The channel to declare the exchange on.</param>
    /// <param name="options">The RabbitMQ options with exchange configuration.</param>
    /// <param name="cancellationToken">Cancellation token for this operation.</param>
    public static async ValueTask ExchangeDeclareAsync(this IChannel channel, RabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(options);

        await channel
            .ExchangeDeclareAsync(
                exchange: options.ExchangeName,
                type: options.ExchangeType,
                durable: options.ExchangeDurable,
                autoDelete: options.AutoDelete,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Declares a queue and binds it to the exchange with the queue name as the routing key.
    /// </summary>
    /// <param name="channel">The channel to declare the queue on.</param>
    /// <param name="queueName">The name of the queue to declare.</param>
    /// <param name="options">The RabbitMQ options with queue configuration.</param>
    /// <param name="cancellationToken">Cancellation token for this operation.</param>
    public static async ValueTask QueueDeclareAsync(this IChannel channel, string queueName, RabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);
        ArgumentNullException.ThrowIfNull(options);

        var queueArguments = options.QueueArguments;
        if (options.QueueType != QueueType.Default)
        {
            queueArguments ??= new Dictionary<string, object?>();
            queueArguments[Headers.XQueueType] = options.QueueType switch
            {
                QueueType.Classic => "classic",
                QueueType.Quorum => "quorum",
                _ => throw new NotSupportedException($"The queue type '{options.QueueType}' is not supported.")
            };
        }

        await channel
            .QueueDeclareAsync(
                queue: queueName,
                durable: options.QueueDurable,
                exclusive: false,
                autoDelete: options.AutoDelete,
                arguments: queueArguments,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        await channel
            .QueueBindAsync(
                queue: queueName,
                exchange: options.ExchangeName,
                routingKey: queueName,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Declares an exchange and a queue, and binds them together with the queue name as the routing key.
    /// </summary>
    /// <param name="channel">The channel to declare the exchange and queue on.</param>
    /// <param name="queueName">The name of the queue to declare.</param>
    /// <param name="options">The RabbitMQ options with exchange and queue configuration.</param>
    /// <param name="cancellationToken">Cancellation token for this operation.</param>
    public static async ValueTask ExchangeAndQueueDeclareAsync(this IChannel channel, string queueName, RabbitMqOptions options, CancellationToken cancellationToken = default)
    {
        await ExchangeDeclareAsync(channel, options, cancellationToken).ConfigureAwait(false);
        await QueueDeclareAsync(channel, queueName, options, cancellationToken).ConfigureAwait(false);
    }
}
