using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");


TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Socket client = server.AcceptSocket(); // wait for client

client.Send(Encoding.UTF8.GetBytes("+PONG\r\n"));