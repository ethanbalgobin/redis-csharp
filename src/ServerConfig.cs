namespace RedisServer;

/// <summary>
/// Configuration settings for the Redis server.
/// </summary>
public class ServerConfig
{
  /// <summary>
  /// Gets or sets the TCP port the server listens on. Defaults to 6379.
  /// </summary>
  public int Port { get; set; } = 6379;
  
  /// <summary>
  /// Gets or sets whether this server is a replica (slave) or master.
  /// </summary>
  public bool IsReplica { get; set; } = false;
  
  /// <summary>
  /// Gets or sets the master host this replica connects to (if IsReplica is true).
  /// </summary>
  public string? MasterHost { get; set; }
  
  /// <summary>
  /// Gets or sets the master port this replica connects to (if IsReplica is true).
  /// </summary>
  public int MasterPort { get; set; }
}