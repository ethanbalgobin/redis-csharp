using System.Collections.Concurrent;
using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using RedisServer;

await MainAsync(args);

/// <summary>
/// Main entry point for the Redis server. Parses command-line arguments and starts the TCP listener.
/// </summary>
/// <param name="args">Command-line arguments (--port, --replicaof)</param>
static async Task MainAsync(string[] args)
{
  var storage = new ConcurrentDictionary<string, (string Value, DateTime? Expiry)>();
  var lists = new ConcurrentDictionary<string, List<string>>();
  var streams = new ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>>();
  var listWaiters = new ConcurrentDictionary<string, List<TaskCompletionSource<string?>>>();

  // Parse command-line arguments
  var config = new ServerConfig();

  for (int i = 0; i < args.Length; i++)
  {
    if (args[i] == "--port" && i + 1 < args.Length)
    {
      if (int.TryParse(args[i + 1], out int parsedPort))
      {
        config.Port = parsedPort;
      }
    }
    else if (args[i] == "--replicaof" && i + 1 < args.Length)
    {
      var parts = args[i + 1].Split(' ');
      if (parts.Length == 2)
      {
        config.IsReplica = true;
        config.MasterHost = parts[0];
        if (int.TryParse(parts[1], out int masterPort))
        {
          config.MasterPort = masterPort;
        }
      }
    }
  }

  // If this is a replica, initiate handshake with master
  if (config.IsReplica && config.MasterHost != null)
  {
    _ = InitiateReplicationHandshakeAsync(config, storage, lists, streams);
  }

  TcpListener server = new TcpListener(IPAddress.Any, config.Port);
  server.Start();

  while (true)
  {
    var client = await server.AcceptTcpClientAsync();
    _ = HandleClientAsync(client, storage, lists, streams, listWaiters, config, CancellationToken.None);
  }
}

/// <summary>
/// Initiates the replication handshake with the master server.
/// Sends PING, then two REPLCONF commands (listening-port and capa) and PSYNC.
/// After handshake, reads RDB file and starts listening for propagated commands.
/// </summary>
/// <param name="config">Server configuration containing master host and port</param>
/// <param name="storage">Key-value storage for strings</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
static async Task InitiateReplicationHandshakeAsync(
  ServerConfig config,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams)
{
  try
  {
    var client = new TcpClient();
    await client.ConnectAsync(config.MasterHost!, config.MasterPort);

    var stream = client.GetStream();

    // Send ping as RESP encoded array
    string pingCommand = "*1\r\n$4\r\nPING\r\n";
    byte[] pingBytes = Encoding.UTF8.GetBytes(pingCommand);
    await stream.WriteAsync(pingBytes, 0, pingBytes.Length);

    // Read response
    var buffer = new byte[1024];
    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
    string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);

    // Send REPLCONF listening-port <PORT>
    string portString = config.Port.ToString();
    string replconfPort = $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portString.Length}\r\n{portString}\r\n";
    byte[] replconfPortBytes = Encoding.UTF8.GetBytes(replconfPort);
    await stream.WriteAsync(replconfPortBytes, 0, replconfPortBytes.Length);

    // Read REPLCONF listening-port response (expecting +OK\r\n)
    bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
    string replconfPortResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);

    // Send REPLCONF capa psync2
    string replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    byte[] replconfCapaBytes = Encoding.UTF8.GetBytes(replconfCapa);
    await stream.WriteAsync(replconfCapaBytes, 0, replconfCapaBytes.Length);

    // Read REPLCONF capa response (expecting +OK\r\n)
    bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
    string replconfCapaResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);

    // Send PSYNC ? -1
    string psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    byte[] psyncBytes = Encoding.UTF8.GetBytes(psyncCommand);
    await stream.WriteAsync(psyncBytes, 0, psyncBytes.Length);

    // Read PSYNC response (expecting +FULLRESYNC <REPL_ID> 0\r\n)
    // The response might also contain the start of the RDB file
    bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
    string psyncResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);

    // Find where the PSYNC response ends
    int psyncEndIdx = psyncResponse.IndexOf("\r\n");
    if (psyncEndIdx >= 0)
    {
      // Extract any remaining data after the PSYNC response
      int remainingStart = psyncEndIdx + 2;
      byte[] remainingData = new byte[bytesRead - remainingStart];
      Array.Copy(buffer, remainingStart, remainingData, 0, remainingData.Length);

      // Read and discard the RDB file, passing any already-read data
      byte[] extraData = await ReadRdbFileAsync(stream, buffer, remainingData);

      // Start listening for propagated commands from master, passing any extra data
      await ListenForPropagatedCommandsAsync(stream, extraData, config, storage, lists, streams);
    }
  }
  catch (Exception ex)
  {
    Console.WriteLine($"Replication handshake failed: {ex.Message}");
  }
}


/// <summary>
/// Handles a single client connection, reading commands and sending responses.
/// Detects PSYNC command to send RDB file for replication.
/// </summary>
/// <param name="client">The TCP client connection</param>
/// <param name="storage">Key-value storage for strings with optional expiry</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
/// <param name="listWaiters">Task completion sources for blocking list operations</param>
/// <param name="config">Server configuration including port and replication settings</param>
/// <param name="ct">Cancellation token</param>
static async Task HandleClientAsync(
  TcpClient client,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters,
  ServerConfig config,
  CancellationToken ct)
{
  using var _ = client;
  var stream = client.GetStream();
  var buffer = new byte[1024];
  var transactionState = new TransactionState();
  bool isReplicaConnection = false;

  try
  {
    while (true)
    {
      int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct);
      if (bytesRead <= 0) return;

      string request = Encoding.UTF8.GetString(buffer, 0, bytesRead);
      var command = RedisHelpers.ParseRespArray(request);

      byte[] response = await ProcessCommand(command, storage, lists, streams, listWaiters, transactionState, config);
      await stream.WriteAsync(response, 0, response.Length, ct);

      // if this was a PSYNC command, send the RDB file
      if (command.Length > 0 && command[0].ToUpper() == "PSYNC")
      {
        await SendEmptyRdbFileAsync(stream, ct);

        lock (config.ReplicaStreams)
        {
          config.ReplicaStreams.Add(stream);
        }
        isReplicaConnection = true;
      }
    }
  }
  catch
  {
    // Handle exceptions silently
  }
  finally
  {
    if (isReplicaConnection)
    {
      lock (config.ReplicaStreams)
      {
        config.ReplicaStreams.Remove(stream);
      }
    }
  }
}

/// <summary>
/// Sends an empty RDB file to a replica during full resynchronization.
/// The file is sent in the format: $<length>\r\n<binary_contents>
/// </summary>
/// <param name="stream">Network stream to write to</param>
/// <param name="ct">Cancellation token</param> 
static async Task SendEmptyRdbFileAsync(NetworkStream stream, CancellationToken ct)
{
  // Empty RDB file in hex format
  string emptyRdbHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

  // Convert hex string to byte array
  byte[] rdbBytes = Convert.FromHexString(emptyRdbHex);

  string header = $"${rdbBytes.Length}\r\n";
  byte[] headerBytes = Encoding.UTF8.GetBytes(header);

  await stream.WriteAsync(headerBytes, 0, headerBytes.Length, ct);
  await stream.WriteAsync(rdbBytes, 0, rdbBytes.Length, ct);
}

/// <summary>
/// Processes a Redis command and returns the appropriate RESP-encoded response.
/// Handles transaction queuing and execution.
/// </summary>
/// <param name="command">Parsed command array</param>
/// <param name="storage">Key-value storage for strings</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
/// <param name="listWaiters">Task completion sources for blocking operations</param>
/// <param name="transactionState">Current transaction state for this connection</param>
/// <param name="config">Server configuration</param>
/// <returns>RESP-encoded response bytes</returns>
static async Task<byte[]> ProcessCommand(
  string[] command,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters,
  TransactionState transactionState,
  ServerConfig config)
{
  if (command.Length == 0)
    return Encoding.UTF8.GetBytes("+PONG\r\n");

  string cmd = command[0].ToUpper();

  // If in transaction, queue commands (except MULTI, EXEC, DISCARD)
  if (transactionState.InTransaction && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD")
  {
    transactionState.QueuedCommands.Add(command);
    return Encoding.UTF8.GetBytes("+QUEUED\r\n");
  }

  // Execute commands
  return cmd switch
  {
    "PING" => HandlePing(),
    "ECHO" when command.Length > 1 => HandleEcho(command[1]),
    "LPOP" when command.Length >= 2 => HandleLPop(command, lists),
    "LLEN" when command.Length == 2 => HandleLLen(command, lists),
    "GET" when command.Length >= 2 => HandleGet(command[1], storage),
    "SET" when command.Length >= 3 => HandleSet(command, storage, config),
    "INCR" when command.Length >= 2 => HandleIncr(command[1], storage),
    "MULTI" => HandleMulti(transactionState),
    "EXEC" => await HandleExecAsync(command, storage, lists, streams, listWaiters, transactionState, config),
    "DISCARD" => HandleDiscard(transactionState),
    "INFO" => HandleInfo(command, config),
    "REPLCONF" => HandleReplconf(command),
    "PSYNC" => HandlePsync(command),
    "WAIT" when command.Length >= 3 => HandleWait(config),
    "RPUSH" when command.Length >= 3 => HandleRPush(command, lists, listWaiters),
    "LPUSH" when command.Length >= 3 => HandleLPush(command, lists, listWaiters),
    "LRANGE" when command.Length >= 4 => HandleLRange(command, lists),
    "BLPOP" when command.Length >= 3 => await HandleBLPopAsync(command, lists, listWaiters),
    "TYPE" when command.Length == 2 => HandleGetType(command, storage, lists, streams),
    "XADD" when command.Length >= 4 => HandleXAdd(command, streams, listWaiters),
    "XRANGE" when command.Length >= 4 => HandleXRange(command, streams),
    "XREAD" when command.Length >= 4 => await HandleXReadAsync(command, streams, listWaiters),
    _ => Encoding.UTF8.GetBytes("+PONG\r\n")
  };
}

/// <summary>
/// Handles the PING command.
/// </summary>
/// <returns>PONG response as a simple string</returns>
static byte[] HandlePing()
{
  return Encoding.UTF8.GetBytes("+PONG\r\n");
}

/// <summary>
/// Handles the ECHO command, returning the provided message.
/// </summary>
/// <param name="message">Message to echo back</param>
/// <returns>Message encoded as a bulk string</returns>
static byte[] HandleEcho(string message)
{
  return Encoding.UTF8.GetBytes($"${message.Length}\r\n{message}\r\n");
}

/// <summary>
/// Handles the SET command, storing a key-value pair with optional expiry.
/// </summary>
/// <param name="command">Command array (SET key value [EX seconds | PX milliseconds])</param>
/// <param name="storage">Key-value storage</param>
/// <returns>OK response as a simple string</returns>
static byte[] HandleSet(string[] command, ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage, ServerConfig config)
{
  string key = command[1];
  string value = command[2];
  DateTime? expiry = null;

  if (command.Length >= 5)
  {
    string option = command[3].ToUpper();
    if (int.TryParse(command[4], out int time))
    {
      expiry = option switch
      {
        "PX" => DateTime.UtcNow.AddMilliseconds(time),
        "EX" => DateTime.UtcNow.AddSeconds(time),
        _ => null
      };
    }
  }

  storage[key] = (value, expiry);

  if (!config.IsReplica)
  {
    RedisHelpers.PropagateCommandToReplicas(command, config);
  }

  return Encoding.UTF8.GetBytes("+OK\r\n");
}

/// <summary>
/// Handles the GET command, retrieving a value by key.
/// </summary>
/// <param name="key">Key to retrieve</param>
/// <param name="storage">Key-value storage</param>
/// <returns>Value as bulk string, or null bulk string if key doesn't exist or is expired</returns>
static byte[] HandleGet(string key, ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage)
{
  if (!storage.TryGetValue(key, out var entry))
    return Encoding.UTF8.GetBytes("$-1\r\n");

  if (entry.Expiry.HasValue && DateTime.UtcNow > entry.Expiry.Value)
  {
    storage.TryRemove(key, out _);
    return Encoding.UTF8.GetBytes("$-1\r\n");
  }

  return Encoding.UTF8.GetBytes($"${entry.Value.Length}\r\n{entry.Value}\r\n");
}

/// <summary>
/// Handles the INCR command, incrementing an integer value by 1.
/// </summary>
/// <param name="key">Key to increment</param>
/// <param name="storage">Key-value storage</param>
/// <returns>New value as an integer, or error if value is not an integer</returns>
static byte[] HandleIncr(string key, ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage)
{
  if (!storage.TryGetValue(key, out var entry))
  {
    storage[key] = ("1", null);
    return Encoding.UTF8.GetBytes(":1\r\n");
  }

  if (entry.Expiry.HasValue && DateTime.UtcNow > entry.Expiry.Value)
  {
    storage.TryRemove(key, out _);
    storage[key] = ("1", null);
    return Encoding.UTF8.GetBytes(":1\r\n");
  }

  if (!int.TryParse(entry.Value, out int value))
  {
    return Encoding.UTF8.GetBytes("-ERR value is not an integer or out of range\r\n");
  }

  value++;
  storage[key] = (value.ToString(), entry.Expiry);
  
  return Encoding.UTF8.GetBytes($":{value}\r\n");
}

/// <summary>
/// Handles the MULTI command, starting a transaction.
/// </summary>
/// <param name="transactionState">Transaction state to update</param>
/// <returns>OK response as a simple string</returns>
static byte[] HandleMulti(TransactionState transactionState)
{
  transactionState.InTransaction = true;
  return Encoding.UTF8.GetBytes("+OK\r\n");
}

/// <summary>
/// Handles the EXEC command, executing all queued commands in a transaction.
/// </summary>
/// <param name="originalCommand">The original EXEC command</param>
/// <param name="storage">Key-value storage</param>
/// <param name="lists">List storage</param>
/// <param name="streams">Stream storage</param>
/// <param name="listWaiters">Blocking operation waiters</param>
/// <param name="transactionState">Transaction state</param>
/// <param name="config">Server configuration</param>
/// <returns>Array of responses from queued commands, or error if not in transaction</returns>
static async Task<byte[]> HandleExecAsync(
  string[] originalCommand,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters,
  TransactionState transactionState,
  ServerConfig config)
{
  if (!transactionState.InTransaction)
  {
    return Encoding.UTF8.GetBytes("-ERR EXEC without MULTI\r\n");
  }

  var queuedCommands = new List<string[]>(transactionState.QueuedCommands);
  
  transactionState.InTransaction = false;
  transactionState.QueuedCommands.Clear();

  if (queuedCommands.Count == 0)
  {
    return Encoding.UTF8.GetBytes("*0\r\n");
  }

  var responses = new List<byte[]>();
  var tempTransactionState = new TransactionState();
  
  foreach (var cmd in queuedCommands)
  {
    byte[] response = await ProcessCommand(cmd, storage, lists, streams, listWaiters, tempTransactionState, config);
    responses.Add(response);
  }

  var result = new StringBuilder();
  result.Append($"*{responses.Count}\r\n");
  
  foreach (var response in responses)
  {
    result.Append(Encoding.UTF8.GetString(response));
  }

  return Encoding.UTF8.GetBytes(result.ToString());
}

/// <summary>
/// Handles the DISCARD command, aborting a transaction and discarding queued commands.
/// </summary>
/// <param name="transactionState">Transaction state to clear</param>
/// <returns>OK response, or error if not in transaction</returns>
static byte[] HandleDiscard(TransactionState transactionState)
{
  if (!transactionState.InTransaction)
  {
    return Encoding.UTF8.GetBytes("-ERR DISCARD without MULTI\r\n");
  }

  transactionState.InTransaction = false;
  transactionState.QueuedCommands.Clear();

  return Encoding.UTF8.GetBytes("+OK\r\n");
}

/// <summary>
/// Handles the INFO command, returning server information.
/// For replication section, includes role, master_replid, and master_repl_offset.
/// </summary>
/// <param name="command">Command array (INFO [section])</param>
/// <param name="config">Server configuration</param>
/// <returns>Information as a bulk string</returns>
static byte[] HandleInfo(string[] command, ServerConfig config)
{
  string section = command.Length > 1 ? command[1].ToLower() : "";

  if (section == "replication" || section == "")
  {
    var lines = new List<string>();

    if (config.IsReplica)
    {
      lines.Add("role:slave");
    }
    else
    {
      lines.Add("role:master");
      lines.Add("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
      lines.Add("master_repl_offset:0");
    }

    string response = string.Join("\r\n", lines);
    return Encoding.UTF8.GetBytes($"${response.Length}\r\n{response}\r\n");
  }

  return Encoding.UTF8.GetBytes("$-1\r\n");
}

/// <summary>
/// Handles the REPLCONF command from a replica during replication handshake.
/// Accepts listening-port and capa arguments and responds with OK. 
/// </summary>
/// <param name="Command">Command array (REPLCONF [listening-port PORT | capa CAPABILITY])</param>
/// <returns>OK response as a simple string</returns>
static byte[] HandleReplconf(string[] command)
{
  return Encoding.UTF8.GetBytes("+OK\r\n");
}

/// <summary>
/// Handles the PSYNC command from a replica during replication handshake.
/// Responds with FULLRESYNC containing the master's replication ID and offset.
/// </summary>
/// <param name="command">Command array (PSYNC replication_id offset)</param>
/// <returns>FULLRESYNC response as a simple string</returns>
static byte[] HandlePsync(string[] command)
{
  string replId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  int offset = 0;

  string response = $"+FULLRESYNC {replId} {offset}\r\n";
  return Encoding.UTF8.GetBytes(response);
}

/// <summary>
/// Handles the RPUSH command, appending values to the end of a list.
/// </summary>
/// <param name="command">Command array (RPUSH key value [value ...])</param>
/// <param name="lists">List storage</param>
/// <param name="listWaiters">Blocking operation waiters to notify</param>
/// <returns>Length of list after push as an integer</returns>
static byte[] HandleRPush(string[] command, ConcurrentDictionary<string, List<string>> lists, ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  string key = command[1];
  var list = lists.GetOrAdd(key, _ => new List<string>());

  int count;
  lock (list)
  {
    for (int i = 2; i < command.Length; i++)
    {
      list.Add(command[i]);
    }
    count = list.Count;
  }

  NotifyWaiters(key, lists, listWaiters);

  return Encoding.UTF8.GetBytes($":{count}\r\n");
}

/// <summary>
/// Handles the LPUSH command, prepending values to the start of a list.
/// </summary>
/// <param name="command">Command array (LPUSH key value [value ...])</param>
/// <param name="lists">List storage</param>
/// <param name="listWaiters">Blocking operation waiters to notify</param>
/// <returns>Length of list after push as an integer</returns>
static byte[] HandleLPush(string[] command, ConcurrentDictionary<string, List<string>> lists, ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  string key = command[1];
  var list = lists.GetOrAdd(key, _ => new List<string>());

  int count;
  lock (list)
  {
    for (int i = 2; i < command.Length; i++)
    {
      list.Insert(0, command[i]);
    }
    count = list.Count;
  }

  NotifyWaiters(key, lists, listWaiters);

  return Encoding.UTF8.GetBytes($":{count}\r\n");
}

/// <summary>
/// Handles the LPOP command, removing and returning elements from the start of a list.
/// </summary>
/// <param name="command">Command array (LPOP key [count])</param>
/// <param name="lists">List storage</param>
/// <returns>Popped value(s) as bulk string or array, or null if list is empty</returns>
static byte[] HandleLPop(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];
  int count = 1;

  if (!lists.TryGetValue(key, out var list))
  {
    return Encoding.UTF8.GetBytes("$-1\r\n");
  }

  if (command.Length >= 3 && int.TryParse(command[2], out int parsedCount))
  {
    count = parsedCount;
  }

  lock (list)
  {
    if (list.Count == 0)
    {
      return Encoding.UTF8.GetBytes("$-1\r\n");
    }
    
    if (count == 1)
    {
      var value = list[0];
      list.RemoveAt(0);
      return Encoding.UTF8.GetBytes($"${value.Length}\r\n{value}\r\n");
    }

    int actualCount = Math.Min(count, list.Count);
    var removed = new List<string>();

    for (int i = 0; i < actualCount; i++)
    {
      removed.Add(list[0]);
      list.RemoveAt(0);
    }

    var result = new StringBuilder();
    result.Append($"*{removed.Count}\r\n");

    foreach (var element in removed)
    {
      result.Append($"${element.Length}\r\n{element}\r\n");
    }

    return Encoding.UTF8.GetBytes(result.ToString());
  }
}

/// <summary>
/// Handles the BLPOP command, blocking until a value is available or timeout occurs.
/// </summary>
/// <param name="command">Command array (BLPOP key [key ...] timeout)</param>
/// <param name="lists">List storage</param>
/// <param name="listWaiters">Blocking operation waiters</param>
/// <returns>Array with key and value, or null if timeout</returns>
static async Task<byte[]> HandleBLPopAsync(
  string[] command, 
  ConcurrentDictionary<string, List<string>> lists, 
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  string key = command[1];
  double timeout = 0;

  if (command.Length >= 3 && double.TryParse(command[command.Length - 1], out double parsedTimeout))
  {
    timeout = parsedTimeout;
  }

  while (true)
  {
    if (lists.TryGetValue(key, out var list))
    {
      lock (list)
      {
        if (list.Count > 0)
        {
          var value = list[0];
          list.RemoveAt(0);

          var result = new StringBuilder();
          result.Append("*2\r\n");
          result.Append($"${key.Length}\r\n{key}\r\n");
          result.Append($"${value.Length}\r\n{value}\r\n");

          return Encoding.UTF8.GetBytes(result.ToString());
        }
      }
    }

    var tcs = new TaskCompletionSource<string?>();
    var waiters = listWaiters.GetOrAdd(key, _ => new List<TaskCompletionSource<string?>>());

    lock (waiters)
    {
      waiters.Add(tcs);
    }

    if (timeout > 0)
    {
      var timeoutTask = Task.Delay(TimeSpan.FromSeconds(timeout));
      var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

      if (completedTask == timeoutTask)
      {
        lock (waiters)
        {
          waiters.Remove(tcs);
        }
        return Encoding.UTF8.GetBytes("*-1\r\n");
      }
    }
    else
    {
      await tcs.Task;
    }
  }
}

/// <summary>
/// Notifies waiting BLPOP operations that a list has new elements.
/// </summary>
/// <param name="key">List key that was modified</param>
/// <param name="lists">List storage</param>
/// <param name="listWaiters">Blocking operation waiters to notify</param>
static void NotifyWaiters(string key, ConcurrentDictionary<string, List<string>> lists, ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  if (!listWaiters.TryGetValue(key, out var waiters))
    return;

  lock (waiters)
  {
    if (!lists.TryGetValue(key, out var list))
      return;

    lock (list)
    {
      while (waiters.Count > 0 && list.Count > 0)
      {
        var waiter = waiters[0];
        waiters.RemoveAt(0);
        waiter.TrySetResult(key);
      }
    }
  }
}

/// <summary>
/// Handles the LLEN command, returning the length of a list.
/// </summary>
/// <param name="command">Command array (LLEN key)</param>
/// <param name="lists">List storage</param>
/// <returns>List length as an integer</returns>
static byte[] HandleLLen(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];

  if (!lists.TryGetValue(key, out var list)) 
    return Encoding.UTF8.GetBytes(":0\r\n");

  return Encoding.UTF8.GetBytes($":{list.Count}\r\n");
}

/// <summary>
/// Handles the LRANGE command, returning a range of elements from a list.
/// </summary>
/// <param name="command">Command array (LRANGE key start stop)</param>
/// <param name="lists">List storage</param>
/// <returns>Array of elements in the specified range</returns>
static byte[] HandleLRange(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];

  if (!int.TryParse(command[2], out int start) || !int.TryParse(command[3], out int stop))
  {
    return Encoding.UTF8.GetBytes("*0\r\n");
  }

  if (!lists.TryGetValue(key, out var list))
  {
    return Encoding.UTF8.GetBytes("*0\r\n");
  }

  lock (list)
  {
    int length = list.Count;

    if (start < 0)
      start = Math.Max(0, length + start);

    if (stop < 0)
      stop = Math.Max(0, length + stop);

    if (start >= length || start > stop)
      return Encoding.UTF8.GetBytes("*0\r\n");

    if (stop >= length)
      stop = length - 1;

    var result = new StringBuilder();
    int count = stop - start + 1;
    result.Append($"*{count}\r\n");

    for (int i = start; i <= stop; i++)
    {
      string element = list[i];
      result.Append($"${element.Length}\r\n{element}\r\n");
    }

    return Encoding.UTF8.GetBytes(result.ToString());
  }
}

/// <summary>
/// Handles the XADD command, adding an entry to a stream.
/// </summary>
/// <param name="command">Command array (XADD key ID field value [field value ...])</param>
/// <param name="streams">Stream storage</param>
/// <param name="listWaiters">Blocking operation waiters to notify</param>
/// <returns>Generated or validated stream ID as bulk string, or error</returns>
static byte[] HandleXAdd(
  string[] command,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  string key = command[1];
  string id = command[2];

  if (id == "*")
  {
    var stream = streams.GetOrAdd(key, _ => new List<(string, Dictionary<string, string>)>());

    lock (stream)
    {
      long currentTime = RedisHelpers.GetCurrentMilliseconds();
      long sequenceNumber = RedisHelpers.GenerateSequenceNumber(stream, currentTime.ToString());
      id = $"{currentTime}-{sequenceNumber}";

      var fields = new Dictionary<string, string>();
      for (int i = 3; i < command.Length; i += 2)
      {
        if (i + 1 < command.Length)
        {
          fields[command[i]] = command[i + 1];
        }
      }

      stream.Add((id, fields));
    }

    RedisHelpers.NotifyStreamWaiters(key, listWaiters);
    return Encoding.UTF8.GetBytes($"${id.Length}\r\n{id}\r\n");
  }

  if (id.Contains("-*"))
  {
    var parts = id.Split('-');
    string timePart = parts[0];

    var stream = streams.GetOrAdd(key, _ => new List<(string, Dictionary<string, string>)>());

    lock (stream)
    {
      long sequenceNumber = RedisHelpers.GenerateSequenceNumber(stream, timePart);
      id = $"{timePart}-{sequenceNumber}";

      var fields = new Dictionary<string, string>();
      for (int i = 3; i < command.Length; i += 2)
      {
        if (i + 1 < command.Length)
        {
          fields[command[i]] = command[i + 1];
        }
      }

      stream.Add((id, fields));
    }

    RedisHelpers.NotifyStreamWaiters(key, listWaiters);
    return Encoding.UTF8.GetBytes($"${id.Length}\r\n{id}\r\n");
  }

  if (id == "0-0")
  {
    return Encoding.UTF8.GetBytes("-ERR The ID specified in XADD must be greater than 0-0\r\n");
  }

  var fieldsDict = new Dictionary<string, string>();
  for (int i = 3; i < command.Length; i += 2)
  {
    if (i + 1 < command.Length)
    {
      fieldsDict[command[i]] = command[i + 1];
    }
  }

  var streamData = streams.GetOrAdd(key, _ => new List<(string, Dictionary<string, string>)>());

  lock (streamData)
  {
    if (streamData.Count > 0)
    {
      string lastId = streamData[streamData.Count - 1].Id;

      if (RedisHelpers.CompareStreamIds(id, lastId) <= 0)
      {
        return Encoding.UTF8.GetBytes("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
      }
    }
    else
    {
      if (RedisHelpers.CompareStreamIds(id, "0-0") <= 0)
      {
        return Encoding.UTF8.GetBytes("-ERR The ID specified in XADD must be greater than 0-0\r\n");
      }
    }

    streamData.Add((id, fieldsDict));
  }

  RedisHelpers.NotifyStreamWaiters(key, listWaiters);
  return Encoding.UTF8.GetBytes($"${id.Length}\r\n{id}\r\n");
}

/// <summary>
/// Handles the TYPE command, returning the type of value stored at a key.
/// </summary>
/// <param name="command">Command array (TYPE key)</param>
/// <param name="storage">String storage</param>
/// <param name="lists">List storage</param>
/// <param name="streams">Stream storage</param>
/// <returns>Type name as a simple string (string, list, stream, or none)</returns>
static byte[] HandleGetType(
  string[] command,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams)
{
  string key = command[1];

  if (storage.TryGetValue(key, out var entry))
  {
    if (entry.Expiry.HasValue && DateTime.UtcNow > entry.Expiry.Value)
    {
      storage.TryRemove(key, out _);
      return Encoding.UTF8.GetBytes("+none\r\n");
    }
    return Encoding.UTF8.GetBytes("+string\r\n");
  }

  if (lists.TryGetValue(key, out _))
  {
    return Encoding.UTF8.GetBytes("+list\r\n");
  }

  if (streams.TryGetValue(key, out _))
  {
    return Encoding.UTF8.GetBytes("+stream\r\n");
  }

  return Encoding.UTF8.GetBytes("+none\r\n");
}

/// <summary>
/// Handles the XRANGE command, returning a range of entries from a stream.
/// </summary>
/// <param name="command">Command array (XRANGE key start end)</param>
/// <param name="streams">Stream storage</param>
/// <returns>Array of stream entries in the specified range</returns>
static byte[] HandleXRange(
  string[] command,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams)
{
  string key = command[1];
  string startId = command[2];
  string endId = command[3];

  startId = RedisHelpers.NormalizeStreamId(startId, isStart: true);
  endId = RedisHelpers.NormalizeStreamId(endId, isStart: false);

  if (!streams.TryGetValue(key, out var stream))
  {
    return Encoding.UTF8.GetBytes("*0\r\n");
  }

  var result = new StringBuilder();
  var matchingEntries = new List<(string Id, Dictionary<string, string> Fields)>();

  lock (stream)
  {
    foreach (var entry in stream)
    {
      if (RedisHelpers.CompareStreamIds(entry.Id, startId) >= 0 && 
          RedisHelpers.CompareStreamIds(entry.Id, endId) <= 0)
      {
        matchingEntries.Add(entry);
      }
    }
  }

  result.Append($"*{matchingEntries.Count}\r\n");

  foreach (var (id, fields) in matchingEntries)
  {
    result.Append("*2\r\n");
    result.Append($"${id.Length}\r\n{id}\r\n");

    result.Append($"*{fields.Count * 2}\r\n");
    foreach (var (fieldName, fieldValue) in fields)
    {
      result.Append($"${fieldName.Length}\r\n{fieldName}\r\n");
      result.Append($"${fieldValue.Length}\r\n{fieldValue}\r\n");
    }
  }

  return Encoding.UTF8.GetBytes(result.ToString());
}

/// <summary>
/// Handles the XREAD command, reading entries from one or more streams.
/// Supports blocking with BLOCK option and $ special ID.
/// </summary>
/// <param name="command">Command array (XREAD [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...])</param>
/// <param name="streams">Stream storage</param>
/// <param name="listWaiters">Blocking operation waiters</param>
/// <returns>Array of stream entries, or null if blocking timeout occurs</returns>
static async Task<byte[]> HandleXReadAsync(
  string[] command,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  int blockTimeout = -1;
  int commandOffset = 1;

  if (command.Length > 1 && command[1].ToUpper() == "BLOCK")
  {
    if (command.Length > 2 && int.TryParse(command[2], out int timeout))
    {
      blockTimeout = timeout;
      commandOffset = 3;
    }
  }

  int streamsIndex = -1;
  for (int i = commandOffset; i < command.Length; i++)
  {
    if (command[i].ToUpper() == "STREAMS")
    {
      streamsIndex = i;
      break;
    }
  }

  if (streamsIndex == -1 || streamsIndex + 2 >= command.Length)
  {
    return Encoding.UTF8.GetBytes("-ERR wrong number of arguments\r\n");
  }

  int numStreams = (command.Length - streamsIndex - 1) / 2;
  var streamKeys = new List<string>();
  var streamIds = new List<string>();

  for (int i = 0; i < numStreams; i++)
  {
    streamKeys.Add(command[streamsIndex + 1 + i]);
    
    string id = command[streamsIndex + 1 + numStreams + i];
    if (id == "$")
    {
      id = RedisHelpers.GetLastStreamId(streams, streamKeys[i]);
    }
    streamIds.Add(id);
  }

  while (true)
  {
    var streamResults = new List<(string key, List<(string Id, Dictionary<string, string> Fields)> entries)>();

    for (int i = 0; i < streamKeys.Count; i++)
    {
      string key = streamKeys[i];
      string startId = streamIds[i];

      if (!streams.TryGetValue(key, out var stream))
      {
        continue;
      }

      var matchingEntries = new List<(string Id, Dictionary<string, string> Fields)>();

      lock (stream)
      {
        foreach (var entry in stream)
        {
          if (RedisHelpers.CompareStreamIds(entry.Id, startId) > 0)
          {
            matchingEntries.Add(entry);
          }
        }
      }

      if (matchingEntries.Count > 0)
      {
        streamResults.Add((key, matchingEntries));
      }
    }

    if (streamResults.Count > 0)
    {
      return RedisHelpers.BuildXReadResponse(streamResults);
    }

    if (blockTimeout == -1)
    {
      return Encoding.UTF8.GetBytes("*-1\r\n");
    }

    var tasks = new List<Task>();
    var tcsList = new List<TaskCompletionSource<string?>>();

    foreach (var key in streamKeys)
    {
      var tcs = new TaskCompletionSource<string?>();
      var waiters = listWaiters.GetOrAdd(key, _ => new List<TaskCompletionSource<string?>>());

      lock (waiters)
      {
        waiters.Add(tcs);
      }

      tcsList.Add(tcs);
      tasks.Add(tcs.Task);
    }

    Task timeoutTask = blockTimeout > 0
      ? Task.Delay(blockTimeout)
      : Task.Delay(Timeout.Infinite);

    tasks.Add(timeoutTask);

    var completedTask = await Task.WhenAny(tasks);

    for (int i = 0; i < streamKeys.Count; i++)
    {
      var key = streamKeys[i];
      if (listWaiters.TryGetValue(key, out var waiters))
      {
        lock (waiters)
        {
          waiters.Remove(tcsList[i]);
        }
      }
    }

    if (completedTask == timeoutTask)
    {
      return Encoding.UTF8.GetBytes("*-1\r\n");
    }
  }
  
}

/// <summary>
/// Reads and discards the RDB file sent by master during full resynchronization.
/// The RDB file is sent in format: $<length>\r\n<binary_contents>
/// </summary>
/// <param name="stream">Network stream to read from</param>
/// <param name="buffer">Buffer for reading data</param>
/// <param name="initialData">Data already read from previous buffer</param>
/// <returns>Any extra data read beyond the RDB file</returns>
static async Task<byte[]> ReadRdbFileAsync(NetworkStream stream, byte[] buffer, byte[] initialData)
{
  var rdbData = new List<byte>(initialData);

  // read until we have the full RDB header ($<length>\r\n)
  while (true)
  {
    string headerStr = Encoding.UTF8.GetString(rdbData.ToArray());
    if (headerStr.Contains("\r\n"))
    {
      // Parse the length from $<length>\r\n
      int dollarIdx = headerStr.IndexOf('$');
      int crlfIdx = headerStr.IndexOf("\r\n");

      if (dollarIdx >= 0 && crlfIdx > dollarIdx)
      {
        string lengthStr = headerStr.Substring(dollarIdx + 1, crlfIdx - dollarIdx - 1);
        if (int.TryParse(lengthStr, out int rdbLength))
        {
          int headerLength = crlfIdx + 2; // Include \r\n
          // Calculate how many RDB bytes we've already read
          int rdbBytesAlreadyRead = rdbData.Count - headerLength;

          // Total bytes needed (header + RDB content)
          int totalBytesNeeded = headerLength + rdbLength;

          // Read remaining RDB bytes
          while (rdbData.Count < totalBytesNeeded)
          {
            int remaining = totalBytesNeeded - rdbData.Count;
            int toRead = Math.Min(remaining, buffer.Length);

            int bytesRead = await stream.ReadAsync(buffer, 0, toRead);
            if (bytesRead == 0)
              return new byte[0];

            for (int i = 0; i < bytesRead; i++)
            {
              rdbData.Add(buffer[i]);
            }
          }

          // RDB file fully received, extract any extra data beyond it
          if (rdbData.Count > totalBytesNeeded)
          {
            int extraCount = rdbData.Count - totalBytesNeeded;
            byte[] extraData = new byte[extraCount];
            rdbData.CopyTo(totalBytesNeeded, extraData, 0, extraCount);
            return extraData;
          }

          return new byte[0];
        }
      }
    }

    // Need more data
    int read = await stream.ReadAsync(buffer, 0, buffer.Length);
    if (read == 0)
      return new byte[0];

    for (int i = 0; i < read; i++)
    {
      rdbData.Add(buffer[i]);
    }
  }
}

/// <summary>
/// Listens for propagated commands from master and executes them on replica's local storage.
/// Commands are received as RESP arrays and executed without sending responses.
/// </summary>
/// <param name="masterStream">Network stream connected to master</param>
/// <param name="config">Server configuration</param>
/// <param name="storage">Key-value storage for strings</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
/// <param name="initialData">Data already read that may contain commands</param>
static async Task ListenForPropagatedCommandsAsync(
  NetworkStream masterStream,
  byte[] initialData,
  ServerConfig config,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams
  ) {
  var buffer = new byte[4096];
  var incompleteData = new StringBuilder();
  var replicationOffset = new ReplicationOffset { Value = 0 };

  // Process any initial data first
  if (initialData.Length > 0)
  {
    string data = Encoding.UTF8.GetString(initialData);
    incompleteData.Append(data);
    await ProcessReplicaCommands(incompleteData, masterStream, storage, lists, streams, replicationOffset);
  }

  try
  {
    while (true)
    {
      int bytesRead = await masterStream.ReadAsync(buffer, 0, buffer.Length);
      if (bytesRead == 0)
        break;

      string data = Encoding.UTF8.GetString(buffer, 0, bytesRead);
      incompleteData.Append(data);

      await ProcessReplicaCommands(incompleteData, masterStream, storage, lists, streams, replicationOffset);
    }
  }
  catch (Exception ex)
  {
    Console.WriteLine($"Error processing propagated commands: {ex.Message}");
  }
}

/// <summary>
/// Tries to parse a complete RESP command from the buffer.
/// Returns the parsed command and the end position, or null if incomplete.
/// </summary>
/// <param name="data">String data containing RESP protocol</param>
/// <param name="startPos">Starting position in the data</param>
/// <returns>Tuple of (parsed command array, end position) or (null, 0) if incomplete</returns>
static (string[]? command, int endPos) TryParseRespCommand(string data, int startPos)
{
  if (startPos >= data.Length || data[startPos] != '*')
    return (null, 0);

  // Find the first \r\n to get array length
  int firstCrlf = data.IndexOf("\r\n", startPos);
  if (firstCrlf == -1)
    return (null, 0);

  string arrayLengthStr = data.Substring(startPos + 1, firstCrlf - startPos - 1);
  if (!int.TryParse(arrayLengthStr, out int arrayLength))
    return (null, 0);

  var result = new List<string>();
  int pos = firstCrlf + 2; // Skip past \r\n
  for (int i = 0; i < arrayLength; i++)
  {
    // Expect bulk string: $<length>\r\n<data>\r\n
    if (pos >= data.Length || data[pos] != '$')
      return (null, 0);

    int bulkCrlf = data.IndexOf("\r\n", pos);
    if (bulkCrlf == -1)
      return (null, 0);

    string bulkLengthStr = data.Substring(pos + 1, bulkCrlf - pos - 1);
    if (!int.TryParse(bulkLengthStr, out int bulkLength))
      return (null, 0);

    pos = bulkCrlf + 2; // Skip past \r\n
    // Check if we have enough data for the bulk string
    if (pos + bulkLength + 2 > data.Length)
      return (null, 0);

    string value = data.Substring(pos, bulkLength);
    result.Add(value);

    pos += bulkLength + 2; // Skip past data and \r\n
  }

  return (result.ToArray(), pos);
}

/// <summary>
/// Processes complete RESP commands from the buffer, executing them on the replica.
/// Removes processed commands from the buffer.
/// </summary>
/// <param name="incompleteData">StringBuilder containing buffered RESP data</param>
/// <param name="storage">Key-value storage for strings</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
static async Task ProcessReplicaCommands(
  StringBuilder buffer,
  NetworkStream masterStream,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ReplicationOffset replicationOffset
  )
{
  string data = buffer.ToString();
  int position = 0;

  while (position < data.Length)
  {
    if (data[position] != '*')
    {
      position++;
      continue;
    }
    int commandStart = position;
    var (command, endPosition) = TryParseRespCommand(data, position);

    if (command == null)
      break; // incomplete command

    int commandByteLength = endPosition - commandStart;


    bool isGetAck = command.Length >= 2 && 
                    command[0].ToUpper() == "REPLCONF" && 
                    command[1].ToUpper() == "GETACK";

    // Process command and get response if needed
    string? response = ExecuteReplicaCommand(command, storage, lists, streams, replicationOffset.Value);

    if (response != null)
    {
      byte[] responseBytes = Encoding.UTF8.GetBytes(response);
      await masterStream.WriteAsync(responseBytes, 0, responseBytes.Length);
      await masterStream.FlushAsync();
    }

    replicationOffset.Value += commandByteLength;

    position = endPosition;
  }

  buffer.Clear();
  if (position < data.Length)
  {
    buffer.Append(data.Substring(position));
  }
}

/// <summary>
/// Executes a command received from master on the replica's local storage.
/// Returns a response string if the command requires a response (REPLCONF GETACK), otherwise null.
/// </summary>
/// <param name="command">Parsed command array</param>
/// <param name="storage">Key-value storage for strings</param>
/// <param name="lists">Storage for Redis lists</param>
/// <param name="streams">Storage for Redis streams</param>
static string? ExecuteReplicaCommand(
  string[] command,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  long currentOffset)
{
  if (command.Length == 0)
    return null;

  string cmd = command[0].ToUpper();

  switch (cmd)
  {
    case "REPLCONF" when command.Length >= 2 && command[1].ToUpper() == "GETACK":
      {
        // Response with REPLCONF ACK <offset> 
        // use the offset before this command was processed
        string offsetStr = currentOffset.ToString();
        return $"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${offsetStr.Length}\r\n{offsetStr}\r\n";
      }
    case "SET" when command.Length >= 3:
      {
        string key = command[1];
        string value = command[2];
        DateTime? expiry = null;

        // Handle PX option
        if (command.Length >= 5 && command[3].ToUpper() == "PX")
        {
          if (int.TryParse(command[4], out int milliseconds))
          {
            expiry = DateTime.UtcNow.AddMilliseconds(milliseconds);
          }
        }
        // Handle EX option
        else if (command.Length >= 5 && command[3].ToUpper() == "EX")
        {
          if (int.TryParse(command[4], out int seconds))
          {
            expiry = DateTime.UtcNow.AddSeconds(seconds);
          }
        }

        storage[key] = (value, expiry);
        break;
      }

    case "DEL" when command.Length >= 2:
      {
        for (int i = 1; i < command.Length; i++)
        {
          storage.TryRemove(command[i], out _);
        }
        break;
      }

    case "XADD" when command.Length >= 4:
      {
        string key = command[1];
        string id = command[2];

        var fields = new Dictionary<string, string>();
        for (int i = 3; i < command.Length; i += 2)
        {
          if (i + 1 < command.Length)
          {
            fields[command[i]] = command[i + 1];
          }
        }

        var stream = streams.GetOrAdd(key, _ => new List<(string, Dictionary<string, string>)>());
        lock (stream)
        {
          stream.Add((id, fields));
        }
        break;
      }

    case "PING":
      break;

    default:
      // Ignore unknown commands
      break;
  }

  return null;
}

/// <summary>
/// Handles the WAIT command, checking how many replicas have acknowledged all previous write commands.
/// When no write commands are sent, return the number of connected replicas since they are all in sync at offset 0
/// </summary>
/// <param name="command">Command array (WAIT numreplicas timeout)</param>
/// <param name="config">Server configuration containing replica connections</param>
/// <returns>Number of replicas that acknowledged, as a RESP integer</returns>
static byte[] HandleWait(ServerConfig config)
{
  int connectedReplicas = 0;
  lock (config.ReplicaStreams)
  {
    connectedReplicas = config.ReplicaStreams.Count;
  }

  return Encoding.UTF8.GetBytes($":{connectedReplicas}\r\n");
}

