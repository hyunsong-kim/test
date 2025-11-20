public class Node
{
    public string Name { get; }
    public int Capacity;              // 남은 수용량
    public List<Node> Neighbors { get; } = new List<Node>();

    public Node(string name, int capacity)
    {
        Name = name;
        Capacity = capacity;
    }
}

public static class Allocator
{
    // start 노드에서 시작해서 인접한 모든 노드(연결된 컴포넌트 전체)를 탐색하면서
    // Capacity > 0 인 노드를 하나 찾아서 Capacity를 1 줄인다.
    public static bool AllocateOne(Node start)
    {
        if (start == null) return false;

        var q = new Queue<Node>();
        var visited = new HashSet<Node>();

        q.Enqueue(start);
        visited.Add(start);

        while (q.Count > 0)
        {
            var cur = q.Dequeue();

            // 현재 노드에 수용 가능 공간이 있으면 여기서 1개 할당
            if (cur.Capacity > 0)
            {
                cur.Capacity--;
                // 어디에 들어갔는지 로그 찍고 싶으면 여기서 cur.Name 출력
                // Console.WriteLine($"Allocated on {cur.Name}, remain={cur.Capacity}");
                return true;
            }

            // 인접한 모든 노드들을 큐에 넣어서 계속 탐색
            foreach (var next in cur.Neighbors)
            {
                if (!visited.Contains(next))
                {
                    visited.Add(next);
                    q.Enqueue(next);
                }
            }
        }

        // 끝까지 돌았는데도 빈 자리가 없다면 실패
        return false;
    }
}
