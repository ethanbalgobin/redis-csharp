namespace RedisServer;

/// <summary>
/// Represents the transaction state for a client connection.
/// Tracks whether a transaction is active and stores queued commands.
/// </summary>
public class TransactionState
{
  /// <summary>
  /// Gets or sets whether a transaction is currently active (MULTI called).
  /// </summary>
  public bool InTransaction { get; set; }
  
  /// <summary>
  /// Gets the list of commands queued during the transaction.
  /// Commands are added when InTransaction is true and executed on EXEC.
  /// </summary>
  public List<string[]> QueuedCommands { get; set; } = new List<string[]>();
}