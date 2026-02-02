using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;


await MainAsync();

static async Task MainAsync()
{
  var storage = new ConcurrentDictionary<string, string>();

  TcpListener server = new TcpListener(IPAddress.Any, 6379);
  server.Start();
  while (true)
  {
    var client = await server.AcceptTcpClientAsync();
    _ = HandleClientAsync(client, storage, CancellationToken.None);
  }
}
static async Task HandleClientAsync(TcpClient client, ConcurrentDictionary<string, string> storage, CancellationToken ct)
{
  using var _ = client;
  var stream = client.GetStream();
  var buffer = new byte[1024];

  try
  {
    while (true)
    {
      int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
      if (bytesRead <= 0) return;

      string request = Encoding.UTF8.GetString(buffer, 0, bytesRead);
      var command = ParseRespArray(request);

      byte[] response;

      if (command.Length > 0 && command[0].Equals("PING", StringComparison.OrdinalIgnoreCase))
      {
        response = Encoding.UTF8.GetBytes("+PONG\r\n");
      }
      else if (command.Length > 1 && command[0].Equals("ECHO", StringComparison.OrdinalIgnoreCase))
      {
        string message = command[1];

        response = Encoding.UTF8.GetBytes($"${message.Length}\r\n{message}\r\n");
      }
      else if (command.Length >= 3 && command[0].Equals("SET", StringComparison.OrdinalIgnoreCase))
      {
        string key = command[1];
        string value = command[2];
        storage[key] = value;

        response = Encoding.UTF8.GetBytes("+OK\r\n");
      }
      else if (command.Length >= 2 && command[0].Equals("GET", StringComparison.OrdinalIgnoreCase))
      {
        string key = command[1];

        if (storage.TryGetValue(key, out string? value))
        {
          response = Encoding.UTF8.GetBytes($"${value.Length}\r\n{value}\r\n");
        }
        else
        {
          response = Encoding.UTF8.GetBytes("$-1\r\n");
        }
      }
      else
      {
        response = Encoding.UTF8.GetBytes("+PONG\r\n");
      }

      await stream.WriteAsync(response, 0, response.Length, ct);
    }
  }
  catch
  {

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