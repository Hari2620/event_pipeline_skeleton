namespace EventPipeline.Contracts;

/// <summary>
/// The one event this skeleton moves around. `Simulate` and `FailUntilAttempt`
/// aren't real business fields -- they're how the producer tells the consumer's
/// simulated processing logic what to do, so the retry/DLQ/idempotency behavior
/// is deterministic and demonstrable instead of relying on chance.
/// </summary>
public record OrderPlacedEvent(
    string OrderId,
    decimal Amount,
    string Simulate,               // "ok" | "transient_fail" | "permanent_fail"
    int? FailUntilAttempt = null    // for transient_fail: how many attempts fail before it succeeds
);
