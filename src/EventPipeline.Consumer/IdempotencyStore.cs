using Microsoft.Data.Sqlite;

namespace EventPipeline.Consumer;

/// <summary>
/// Persisted (not in-memory) processed-message-id record, so a redelivered
/// duplicate is still caught after the consumer process restarts -- an
/// in-memory HashSet would pass every demo and then quietly stop working the
/// first time the consumer actually crashes and comes back up, which is
/// exactly when idempotency matters most.
///
/// This is a check-then-insert, not a single atomic operation -- fine for one
/// consumer thread (RabbitMQ.Client's EventingBasicConsumer delivers on a
/// single dedicated thread by default), but a deployment with multiple
/// concurrent consumer threads/processes sharing this store would need to
/// make MarkProcessed the race-safe step (rely on the PRIMARY KEY constraint
/// and catch the conflict) instead of trusting the prior HasProcessed read.
/// </summary>
public sealed class IdempotencyStore : IDisposable
{
    private readonly SqliteConnection _connection;

    public IdempotencyStore(string dbPath)
    {
        _connection = new SqliteConnection($"Data Source={dbPath}");
        _connection.Open();

        using var create = _connection.CreateCommand();
        create.CommandText = @"
            CREATE TABLE IF NOT EXISTS processed_messages (
                message_id TEXT PRIMARY KEY,
                processed_at_utc TEXT NOT NULL
            );";
        create.ExecuteNonQuery();
    }

    public bool HasProcessed(string messageId)
    {
        using var select = _connection.CreateCommand();
        select.CommandText = "SELECT 1 FROM processed_messages WHERE message_id = $id";
        select.Parameters.AddWithValue("$id", messageId);
        using var reader = select.ExecuteReader();
        return reader.Read();
    }

    public void MarkProcessed(string messageId)
    {
        using var insert = _connection.CreateCommand();
        insert.CommandText = "INSERT OR IGNORE INTO processed_messages (message_id, processed_at_utc) VALUES ($id, $ts)";
        insert.Parameters.AddWithValue("$id", messageId);
        insert.Parameters.AddWithValue("$ts", DateTime.UtcNow.ToString("o"));
        insert.ExecuteNonQuery();
    }

    public void Dispose() => _connection.Dispose();
}
