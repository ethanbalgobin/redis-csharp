using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks.Sources;

await MainAsync();

static async Task MainAsync()
{
  var storage = new ConcurrentDictionary<string, (string Value, DateTime? Expiry)>();
  var lists = new ConcurrentDictionary<string, List<string>>();

  TcpListener server = new TcpListener(IPAddress.Any, 6379);
  server.Start();
  
  while (true)
  {
    var client = await server.AcceptTcpClientAsync();
    _ = HandleClientAsync(client, storage, lists, CancellationToken.None);
  }
}

static async Task HandleClientAsync(
  TcpClient client, 
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage, 
  ConcurrentDictionary<string, List<string>> lists, 
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
      
      byte[] response = ProcessCommand(command, storage, lists);
      await stream.WriteAsync(response, 0, response.Length, ct);
    }
  }
  catch
  {
    // Handle exceptions silently
  }
}

static byte[] ProcessCommand(
  string[] command, 
  ConcurrentDictionary<string, (string Value, DateTime? Expiry)> storage, 
  ConcurrentDictionary<string, List<string>> lists)
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
    "RPUSH" when command.Length >= 3 => HandleRPush(command, lists),
    "LPUSH" when command.Length >= 3 => HandleLPush(command, lists),
    "LRANGE" when command.Length >= 4 => HandleLRange(command, lists),
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

static byte[] HandleRPush(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];
  var list = lists.GetOrAdd(key, _ => new List<string>());

  lock (list)
  {
    for (int i = 2; i < command.Length; i++)
    {
      list.Add(command[i]);
    }
    return Encoding.UTF8.GetBytes($":{list.Count}\r\n");
  }
}

static byte[] HandleLPush(string[] command, ConcurrentDictionary<string, List<string>> lists)
{
  string key = command[1];
  var list = lists.GetOrAdd(key, _ => new List<string>());

  lock (list)
  {
    for (int i = 2; i < command.Length; i++)
    {
      list.Insert(0, command[i]);
    }
    return Encoding.UTF8.GetBytes($":{list.Count}\r\n");
  }
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

static byte[] HandleLLen(string[] command, ConcurrentDictionary<string, List<string>> lists)
{

  string key = command[1];

  if (!lists.TryGetValue(key, out var list)) return Encoding.UTF8.GetBytes($":0\r\n");

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

    // clamp stop to list length to avoid out of bounds
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