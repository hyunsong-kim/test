class Node
{
    public string Id { get; }
    public int MaxCapacity { get; set; }
    public int Capacity { get; set; }
    public List<Node> Neighbors { get; } = new List<Node>();

    public Node(string id, int maxCapacity)
    {
        Id = id;
        MaxCapacity = maxCapacity;
        Capacity = maxCapacity;
    }

    public override string ToString() => $"{Id} (cap={Capacity}/{MaxCapacity})";
}

class Graph
{
    private readonly Dictionary<string, Node> _nodes = new();

    public Node GetOrCreateNode(string id, int maxCapacity = 0)
    {
        if (_nodes.TryGetValue(id, out var node))
            return node;

        node = new Node(id, maxCapacity);
        _nodes[id] = node;
        return node;
    }

    public Node AddNode(string id, int maxCapacity)
    {
        if (_nodes.TryGetValue(id, out var node))
        {
            node.MaxCapacity = maxCapacity;
            node.Capacity = maxCapacity;
            return node;
        }

        node = new Node(id, maxCapacity);
        _nodes[id] = node;
        return node;
    }

    public void AddUndirectedEdge(string id1, string id2)
    {
        var a = GetOrCreateNode(id1);
        var b = GetOrCreateNode(id2);

        a.Neighbors.Add(b);
        b.Neighbors.Add(a);
    }

    public void InitUsed(string id, int used)
    {
        if (!_nodes.TryGetValue(id, out var node))
            return;

        node.Capacity = node.MaxCapacity - used;
        if (node.Capacity < 0) node.Capacity = 0;
        if (node.Capacity > node.MaxCapacity) node.Capacity = node.MaxCapacity;
    }

    // 🔥 BFS 기반 전체 인접 노드까지 탐색하여 배치
    public Node AllocateOneBfs(string startId)
    {
        if (!_nodes.TryGetValue(startId, out var start))
            return null;

        var q = new Queue<Node>();
        var visited = new HashSet<Node>();

        q.Enqueue(start);
        visited.Add(start);

        while (q.Count > 0)
        {
            var cur = q.Dequeue();

            if (cur.Capacity > 0)
            {
                cur.Capacity--;
                return cur;
            }

            foreach (var next in cur.Neighbors)
            {
                if (visited.Contains(next)) continue;
                visited.Add(next);
                q.Enqueue(next);
            }
        }

        return null; // 전체 탐색했지만 cap 남은 정점 없음
    }

    public void PrintAll()
    {
        foreach (var kv in _nodes)
            Console.WriteLine(kv.Value);
    }
}
