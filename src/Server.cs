using System.Net;
using System.Net.Sockets;
using System.Text;


await MainAsync();

static async Task MainAsync()
{
  TcpListener server = new TcpListener(IPAddress.Any, 6379);
  server.Start();
  while (true)
  {
    var client = await server.AcceptTcpClientAsync();
    _ = HandleClientAsync(client, CancellationToken.None);
  }
}
static async Task HandleClientAsync(TcpClient client, CancellationToken ct) {
  using var _ = client;
  var stream = client.GetStream();
  var buffer = new byte[1024];
  try
  {
    while (true)
    {
      int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
      if (bytesRead <= 0)
        return;
      byte[] pong = Encoding.UTF8.GetBytes("+PONG\r\n");
      await stream.WriteAsync(pong, 0, pong.Length, ct);
    }
  }
  catch
  {

  }
}