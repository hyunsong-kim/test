using System;
using System.Collections.Generic;

class Node
{
    public string Id { get; }
    public int Capacity { get; set; }
    public List<Node> Neighbors { get; } = new List<Node>();

    public Node(string id, int capacity)
    {
        Id = id;
        Capacity = capacity;
    }

    public override string ToString() => $"{Id}(cap={Capacity})";
}

class Graph
{
    private readonly Dictionary<string, Node> _nodes = new Dictionary<string, Node>();

    public Node AddNode(string id, int capacity)
    {
        var node = new Node(id, capacity);
        _nodes[id] = node;
        return node;
    }

    public void AddUndirectedEdge(string id1, string id2)
    {
        var a = _nodes[id1];
        var b = _nodes[id2];
        a.Neighbors.Add(b);
        b.Neighbors.Add(a);
    }

    /// <summary>
    /// startId에서 시작해서
    /// 1) 자기 자신 capacity > 0 이면 거기에 할당
    /// 2) 아니면 인접 노드들 중 capacity > 0 인 첫 노드에 할당
    /// 못 찾으면 null 리턴
    /// </summary>
    public Node AllocateOne(string startId)
    {
        if (!_nodes.TryGetValue(startId, out var start))
            throw new ArgumentException($"존재하지 않는 노드: {startId}");

        // 1. 자기 자신 검사
        if (start.Capacity > 0)
        {
            start.Capacity--;
            return start;
        }

        // 2. 인접 정점들 검사 (간단 버전: 바로 이웃만)
        foreach (var neighbor in start.Neighbors)
        {
            if (neighbor.Capacity > 0)
            {
                neighbor.Capacity--;
                return neighbor;
            }
        }

        // 3. 못 찾음 (자기 + 이웃 모두 꽉 참)
        return null;
    }

    public void PrintAll()
    {
        foreach (var kv in _nodes)
        {
            Console.WriteLine(kv.Value);
        }
    }
}

class Program
{
    static void Main()
    {
        var graph = new Graph();

        // aaa(cap=4), bbb(cap=2) 생성
        graph.AddNode("aaa", 4);
        graph.AddNode("bbb", 2);

        // aaa - bbb 연결
        graph.AddUndirectedEdge("aaa", "bbb");

        Console.WriteLine("초기 상태:");
        graph.PrintAll();
        Console.WriteLine();

        // 예: aaa 기준으로 6번 할당 시도
        for (int i = 0; i < 6; i++)
        {
            var allocated = graph.AllocateOne("aaa");
            Console.WriteLine($"{i + 1}번째 할당: " +
                              (allocated == null ? "실패(모두 꽉 참)" : allocated.Id));
        }

        Console.WriteLine();
        Console.WriteLine("최종 상태:");
        graph.PrintAll();
    }
}
