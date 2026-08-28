using System.Text;
using RabbitMQ.Client;

namespace EventPipeline.Contracts;

/// <summary>
/// The exchange/queue topology, declared identically by both the producer and
/// the consumer at startup (RabbitMQ requires exact argument agreement on a
/// re-declare, so both sides calling this same method is what keeps them
/// consistent -- not a convention, an enforced one).
///
/// This topology and its retry/dead-letter behavior were verified against a
/// live RabbitMQ 3.12 broker with a throwaway Python/pika script before this
/// C# was written -- see README "Verification" for what was actually checked
/// and why (this repo's C# itself could not be compiler-verified in the
/// environment it was built in; the broker mechanics could).
/// </summary>
public static class RabbitTopology
{
    public const string MainExchange = "orders.exchange";
    public const string MainQueue = "orders.queue";
    public const string RetryExchange = "orders.retry.exchange";
    public const string RetryQueue = "orders.retry.queue";
    public const string DlqExchange = "orders.dlq.exchange";
    public const string DlqQueue = "orders.dlq.queue";
    public const string RoutingKey = "order.placed";

    /// <summary>Total attempts allowed (the original delivery plus retries) before a
    /// transient failure is treated as exhausted and dead-lettered.</summary>
    public const int MaxAttempts = 3;

    /// <summary>How long a message waits in the retry queue before the broker
    /// bounces it back to the main exchange. RabbitMQ has no native delayed
    /// delivery -- this TTL-then-dead-letter queue is the standard workaround.</summary>
    public const int RetryTtlMs = 5000;

    public static void Declare(IModel channel)
    {
        // Final resting place: retries exhausted, or a known non-retryable failure.
        channel.ExchangeDeclare(DlqExchange, ExchangeType.Direct, durable: true);
        channel.QueueDeclare(DlqQueue, durable: true, exclusive: false, autoDelete: false);
        channel.QueueBind(DlqQueue, DlqExchange, RoutingKey);

        // Retry hop: park the message for RetryTtlMs, then its own dead-letter
        // config bounces it back onto the main exchange with the same routing key.
        channel.ExchangeDeclare(RetryExchange, ExchangeType.Direct, durable: true);
        channel.QueueDeclare(RetryQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-message-ttl"] = RetryTtlMs,
                ["x-dead-letter-exchange"] = MainExchange,
                ["x-dead-letter-routing-key"] = RoutingKey,
            });
        channel.QueueBind(RetryQueue, RetryExchange, RoutingKey);

        // Main queue: the consumer's Nack(requeue: false) here is what the broker
        // dead-letters onto the retry exchange -- the consumer never publishes a
        // retry itself, it just says "not this queue" and the topology does the rest.
        channel.ExchangeDeclare(MainExchange, ExchangeType.Direct, durable: true);
        channel.QueueDeclare(MainQueue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = RetryExchange,
                ["x-dead-letter-routing-key"] = RoutingKey,
            });
        channel.QueueBind(MainQueue, MainExchange, RoutingKey);
    }

    /// <summary>
    /// RabbitMQ appends one x-death header entry per (queue, reason) pair every
    /// time a message is dead-lettered, each carrying a running `count`. This
    /// reads how many times the message has already cycled through the retry
    /// queue -- i.e. how many retry attempts have already happened. Header
    /// values come back from RabbitMQ.Client as byte[] for strings and List/
    /// Dictionary<string, object> for arrays/tables, which is why this isn't a
    /// simple cast.
    /// </summary>
    public static int RetryAttemptCount(IBasicProperties props)
    {
        if (props.Headers is null || !props.Headers.TryGetValue("x-death", out var raw))
            return 0;

        if (raw is not List<object> deaths)
            return 0;

        foreach (var entry in deaths)
        {
            if (entry is not Dictionary<string, object> death)
                continue;

            var queueName = death.TryGetValue("queue", out var q) && q is byte[] queueBytes
                ? Encoding.UTF8.GetString(queueBytes)
                : null;

            if (queueName == RetryQueue && death.TryGetValue("count", out var countObj) && countObj is long count)
                return (int)count;
        }

        return 0;
    }
}
