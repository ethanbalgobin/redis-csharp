using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using RedisServer;

await MainAsync();

static async Task MainAsync()
{
  var storage = new ConcurrentDictionary<string, (string Value, DateTime? Expiry)>();
  var lists = new ConcurrentDictionary<string, List<string>>();
  var streams = new ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>>();
  var listWaiters = new ConcurrentDictionary<string, List<TaskCompletionSource<string?>>>();

  TcpListener server = new TcpListener(IPAddress.Any, 6379);
  server.Start();
  
  while (true)
  {
    var client = await server.AcceptTcpClientAsync();
    _ = HandleClientAsync(client, storage, lists, streams, listWaiters, CancellationToken.None);
  }
}

static async Task HandleClientAsync(
  TcpClient client,
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage,
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters,
  CancellationToken ct)
{
  using var _ = client;
  var stream = client.GetStream();
  var buffer = new byte[1024];

  try
  {
    while (true)
    {
      int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct);
      if (bytesRead <= 0) return;

      string request = Encoding.UTF8.GetString(buffer, 0, bytesRead);
      var command = RedisHelpers.ParseRespArray(request);
      
      byte[] response = await ProcessCommand(command, storage, lists, streams, listWaiters);
      await stream.WriteAsync(response, 0, response.Length, ct);
    }
  }
  catch
  {
    // Handle exceptions silently
  }
}

static async Task<byte[]> ProcessCommand(
  string[] command, 
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage, 
  ConcurrentDictionary<string, List<string>> lists,
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams,
  ConcurrentDictionary<string, List<TaskCompletionSource<string?>>> listWaiters)
{
  if (command.Length == 0)
    return Encoding.UTF8.GetBytes("+PONG\r\n");

  string cmd = command[0].ToUpper();

  return cmd switch
  {
    "PING" => HandlePing(),
    "ECHO" when command.Length > 1 => HandleEcho(command[1]),
    "LPOP" when command.Length >= 2 => HandleLPop(command, lists),
    "LLEN" when command.Length == 2 => HandleLLen(command, lists),
    "GET" when command.Length >= 2 => HandleGet(command[1], storage),
    "SET" when command.Length >= 3 => HandleSet(command, storage),
    "INCR" when command.Length >= 2 => HandleIncr(command[1], storage),
    "MULTI" => HandleMulti(),
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

static byte[] HandlePing()
{
  return Encoding.UTF8.GetBytes("+PONG\r\n");
}

static byte[] HandleEcho(string message)
{
  return Encoding.UTF8.GetBytes($"${message.Length}\r\n{message}\r\n");
}

static byte[] HandleSet(string[] command, ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage)
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
  return Encoding.UTF8.GetBytes("+OK\r\n");
}

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


static byte[] HandleLLen(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];

  if (!lists.TryGetValue(key, out var list)) 
    return Encoding.UTF8.GetBytes(":0\r\n");

  return Encoding.UTF8.GetBytes($":{list.Count}\r\n");
}

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

    // Replace $ with the last ID in the stream
    string id = command[streamsIndex + 1 + numStreams + i];
    if (id == "$")
    {
      id = RedisHelpers.GetLastStreamId(streams, streamKeys[i]);
    }
    streamIds.Add(id);
  }

  // Try to get results immediately
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

    // If we have results, return them
    if (streamResults.Count > 0)
    {
      return RedisHelpers.BuildXReadResponse(streamResults);
    }

    // If not blocking, return null array
    if (blockTimeout == -1)
    {
      return Encoding.UTF8.GetBytes("*-1\r\n");
    }

    // Block and wait for new entries
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

    // Clean up all waiters
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

    // If timeout, return null
    if (completedTask == timeoutTask)
    {
      return Encoding.UTF8.GetBytes("*-1\r\n");
    }

    // loop again to collect results
  }
}

static byte[] HandleMulti()
{
  return Encoding.UTF8.GetBytes("+OK\r\n");
}