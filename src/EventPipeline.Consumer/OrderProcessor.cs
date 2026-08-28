using EventPipeline.Contracts;

namespace EventPipeline.Consumer;

/// <summary>Thrown for a failure that might succeed on retry (a downstream
/// timeout, a transient lock) -- the messaging layer routes these through the
/// retry-TTL loop.</summary>
public sealed class TransientProcessingException(string message) : Exception(message);

/// <summary>Thrown for a failure retrying can never fix (bad data, a
/// business-rule violation) -- the messaging layer skips retries entirely and
/// dead-letters immediately, instead of wasting three attempts and 10+
/// seconds finding out what was already knowable on attempt one.</summary>
public sealed class PoisonMessageException(string message) : Exception(message);

/// <summary>
/// Stand-in "business logic." A real handler wouldn't take a retryAttemptCount
/// parameter or know about x-death at all -- it would just call a real
/// downstream system that's actually sometimes flaky. This one uses
/// FailUntilAttempt purely so the demo's retry behavior is deterministic
/// instead of dependent on real, unrepeatable flakiness.
/// </summary>
public static class OrderProcessor
{
    public static void Process(OrderPlacedEvent evt, int retryAttemptCount)
    {
        switch (evt.Simulate)
        {
            case "ok":
                Console.WriteLine($"  processed order {evt.OrderId} successfully");
                return;

            case "permanent_fail":
                throw new PoisonMessageException($"order {evt.OrderId} failed validation and will never succeed");

            case "transient_fail":
                var failUntil = evt.FailUntilAttempt ?? 1;
                if (retryAttemptCount < failUntil)
                    throw new TransientProcessingException($"order {evt.OrderId} hit a transient downstream error (attempt {retryAttemptCount})");

                var attemptWord = retryAttemptCount == 1 ? "retry" : "retries";
                Console.WriteLine($"  processed order {evt.OrderId} successfully after {retryAttemptCount} {attemptWord}");
                return;

            default:
                throw new PoisonMessageException($"unrecognized simulate value '{evt.Simulate}'");
        }
    }
}
