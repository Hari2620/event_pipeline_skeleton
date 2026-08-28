using System.Text.Json;
using System.Threading; // ManualResetEvent -- not covered by ImplicitUsings (only System.Threading.Tasks is)
using EventPipeline.Consumer;
using EventPipeline.Contracts;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory { HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
RabbitTopology.Declare(channel);
channel.BasicQos(prefetchSize: 0, prefetchCount: 10, global: false);

using var store = new IdempotencyStore(Environment.GetEnvironmentVariable("IDEMPOTENCY_DB_PATH") ?? "idempotency.db");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (_, ea) =>
{
    var messageId = ea.BasicProperties.MessageId ?? ea.DeliveryTag.ToString();

    if (store.HasProcessed(messageId))
    {
        Console.WriteLine($"[dup]       {messageId} already processed -- skipping, ack");
        channel.BasicAck(ea.DeliveryTag, multiple: false);
        return;
    }

    var evt = JsonSerializer.Deserialize<OrderPlacedEvent>(ea.Body.Span)
              ?? throw new PoisonMessageException("could not deserialize message body");
    var attempts = RabbitTopology.RetryAttemptCount(ea.BasicProperties);

    try
    {
        OrderProcessor.Process(evt, attempts);
        store.MarkProcessed(messageId);
        channel.BasicAck(ea.DeliveryTag, multiple: false);
    }
    catch (PoisonMessageException ex)
    {
        Console.WriteLine($"[poison]    {messageId} {ex.Message} -- dead-lettering immediately, no retry");
        DeadLetter(channel, ea, $"poison: {ex.Message}");
        channel.BasicAck(ea.DeliveryTag, multiple: false);
    }
    catch (TransientProcessingException ex)
    {
        if (attempts + 1 >= RabbitTopology.MaxAttempts)
        {
            Console.WriteLine($"[exhausted] {messageId} {ex.Message} -- {attempts + 1} attempts made, dead-lettering");
            DeadLetter(channel, ea, $"exhausted retries: {ex.Message}");
            channel.BasicAck(ea.DeliveryTag, multiple: false);
        }
        else
        {
            Console.WriteLine($"[retry]     {messageId} {ex.Message} -- routing to retry queue "
                               + $"({RabbitTopology.RetryTtlMs}ms delay)");
            channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
        }
    }
};

channel.BasicConsume(RabbitTopology.MainQueue, autoAck: false, consumer);

Console.WriteLine("Consumer running. Ctrl+C to exit.");
var exitSignal = new ManualResetEvent(false);
Console.CancelKeyPress += (_, e) => { e.Cancel = true; exitSignal.Set(); };
exitSignal.WaitOne();

static void DeadLetter(IModel channel, BasicDeliverEventArgs ea, string reason)
{
    var props = channel.CreateBasicProperties();
    props.MessageId = ea.BasicProperties.MessageId;
    props.DeliveryMode = 2;
    props.Headers = new Dictionary<string, object> { ["x-dead-letter-reason"] = reason };

    channel.BasicPublish(RabbitTopology.DlqExchange, RabbitTopology.RoutingKey, props, ea.Body);
}
