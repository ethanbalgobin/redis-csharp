namespace RedisServer;

class TransactionState
{
  public bool InTransaction { get; set; }
  public List<string[]> QueuedCommands { get; set; } = new List<string[]>();
}