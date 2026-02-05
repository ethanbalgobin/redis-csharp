namespace RedisServer;

public static class RedisHelpers
{
  public static (long milliseconds, long sequence) ParseStreamId(string id)
  {
    var parts = id.Split('-');
    if (parts.Length != 2)
      return (0, 0);

    long.TryParse(parts[0], out long milliseconds);
    long.TryParse(parts[1], out long sequence);

    return (milliseconds, sequence);
  }

  public static int CompareStreamIds(string id1, string id2)
  {
    var (ms1, seq1) = ParseStreamId(id1);
    var (ms2, seq2) = ParseStreamId(id2);

    if (ms1 != ms2)
      return ms1.CompareTo(ms2);

    return seq1.CompareTo(seq2);
  }

  public static string NormalizeStreamId(string id, bool isStart)
  {
    if (id.Contains("-"))
      return id;

    return isStart ? $"{id}-0" : $"{id}-{long.MaxValue}";
  }

  public static long GetCurrentMilliseconds()
  {
    return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
  }

  public static long GenerateSequenceNumber(
    List<(string Id, Dictionary<string, string> Fields)> stream, 
    string timePart)
  {
    if (timePart == "0")
      return 1;

    for (int i = stream.Count - 1; i >= 0; i--)
    {
      var (ms, seq) = ParseStreamId(stream[i].Id);

      if (ms.ToString() == timePart)
        return seq + 1;

      if (ms < long.Parse(timePart))
        break;
    }

    return 0;
  }

  public static string[] ParseRespArray(string input)
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
}