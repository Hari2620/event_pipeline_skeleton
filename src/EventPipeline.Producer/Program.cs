using System.Text;
using System.Text.Json;
using EventPipeline.Contracts;
using RabbitMQ.Client;

int count = 20;
foreach (var arg in args)
{
    if (arg.StartsWith("--count=", StringComparison.Ordinal))
        count = int.Parse(arg["--count=".Length..]);
}

var factory = new ConnectionFactory { HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost" };
using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();
RabbitTopology.Declare(channel);

var rng = new Random(42);
var published = new List<(Guid Id, OrderPlacedEvent Evt)>();

for (var i = 0; i < count; i++)
{
    var messageId = Guid.NewGuid();
    var roll = rng.NextDouble();

    // Mixed scenario: mostly normal traffic, some transient failures that
    // should recover after a retry or two, a few that should never succeed.
    var evt = roll switch
    {
        < 0.70 => new OrderPlacedEvent($"order-{i}", 42.50m, "ok"),
        < 0.85 => new OrderPlacedEvent($"order-{i}", 42.50m, "transient_fail", rng.Next(1, 3)),
        _ => new OrderPlacedEvent($"order-{i}", 42.50m, "permanent_fail"),
    };

    published.Add((messageId, evt));
    Publish(channel, messageId, evt);
}

// Always demonstrate idempotency explicitly, regardless of how the random mix
// landed: re-publish an earlier "ok" message under its *original* message id.
var firstOk = published.FirstOrDefault(p => p.Evt.Simulate == "ok");
if (firstOk != default)
{
    Console.WriteLine($"re-publishing {firstOk.Id} as an exact duplicate to exercise the consumer's idempotency check");
    Publish(channel, firstOk.Id, firstOk.Evt);
}

Console.WriteLine($"\npublished {count} messages (+1 duplicate). Start the consumer to process them.");

static void Publish(IModel channel, Guid messageId, OrderPlacedEvent evt)
{
    var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(evt));
    var props = channel.CreateBasicProperties();
    props.MessageId = messageId.ToString();
    props.DeliveryMode = 2; // persistent
    props.ContentType = "application/json";

    channel.BasicPublish(RabbitTopology.MainExchange, RabbitTopology.RoutingKey, props, body);
    Console.WriteLine($"published {messageId} order={evt.OrderId} simulate={evt.Simulate}");
}
