using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;

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
      var command = ParseRespArray(request);
      
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
    "RPUSH" when command.Length >= 3 => HandleRPush(command, lists, listWaiters),
    "LPUSH" when command.Length >= 3 => HandleLPush(command, lists, listWaiters),
    "LRANGE" when command.Length >= 4 => HandleLRange(command, lists),
    "BLPOP" when command.Length >= 3 => await HandleBLPopAsync(command, lists, listWaiters),
    "TYPE" when command.Length == 2 => HandleGetType(command, storage, lists, streams),
    "XADD" when command.Length >= 4 => HandleXAdd(command, streams),
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
      // Notify one waiter per available element
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
  ConcurrentDictionary<string, List<(string Id, Dictionary<string, string> Fields)>> streams
)
{
  string key = command[1];
  string id = command[2];

  if (id == "0-0")
  {
    return Encoding.UTF8.GetBytes("-ERR The ID specified in XADD must be greater than 0-0\r\n");
  }

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
    if (stream.Count > 0)
    {
      string lastId = stream[stream.Count - 1].Id;

      if (CompareStreamIds(id, lastId) <= 0)
      {
        return Encoding.UTF8.GetBytes("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
      }
    }
    else
    {
      if (CompareStreamIds(id, "0-0") <= 0)
      {
        return Encoding.UTF8.GetBytes("-ERR The specified ID in XADD must be greater than 0-0\r\n");
      }
    }

    stream.Add((id, fields));

    return Encoding.UTF8.GetBytes($"${id.Length}\r\n{id}\r\n");
  }
}

static (long milliseconds, long sequence) ParseStreamId(string id)
{
  var parts = id.Split('-');
  if (parts.Length != 2)
    return (0, 0);

  long.TryParse(parts[0], out long milliseconds);
  long.TryParse(parts[1], out long sequence);

  return (milliseconds, sequence);
}

static int CompareStreamIds(string id1, string id2)
{
  var (ms1, seq1) = ParseStreamId(id1);
  var (ms2, seq2) = ParseStreamId(id2);

  if (ms1 != ms2)
    return ms1.CompareTo(ms2);

  return seq1.CompareTo(seq2);

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
      return Encoding.UTF8.GetBytes("+none\r\n");
    }
    return Encoding.UTF8.GetBytes($"+{entry.Value.GetType().Name.ToLower()}\r\n");
  }

  if (lists.TryGetValue(key, out _))
  {
    return Encoding.UTF8.GetBytes("+list\r\n");
  }

  if (streams.TryGetValue(key, out _))
  {
    return Encoding.UTF8.GetBytes("+stream\r\n");
  }

  // key does not exist
  return Encoding.UTF8.GetBytes("+none\r\n");
}

static string[] ParseRespArray(string input)
{
  var lines = input.Split(new[] { "\r\n" }, StringSplitOptions.None);
  var result = new List<string>();

  for (int i = 0; i < lines.Length; i++)
  {
    if (lines[i].StartsWith("$") && i + 1 < lines.Length)
    {
      result.Add(lines[i + 1]);
      i++;
    }
  }

  return result.ToArray();
}