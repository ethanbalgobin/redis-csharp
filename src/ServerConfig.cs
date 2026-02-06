namespace RedisServer;

public class ServerConfig
{
  public int Port { get; set; } = 6379;
  public bool IsReplica { get; set; } = false;
  public string? MasterHost { get; set; }
  public int MasterPort { get; set; }
  
}