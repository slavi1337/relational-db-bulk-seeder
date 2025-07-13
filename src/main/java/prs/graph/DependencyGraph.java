package prs.graph;

import java.util.*;

public class DependencyGraph {

    private final Map<String, List<String>> graph;

    public DependencyGraph(Map<String, List<String>> dependencies) {
        this.graph = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
            String child = entry.getKey().toLowerCase();
            List<String> parentsLC = new ArrayList<>();
            for (String p : entry.getValue()) {
                parentsLC.add(p.toLowerCase());
            }
            this.graph.put(child, parentsLC);
        }
    }

    public List<String> topologicalSort(List<String> allTables) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> adjList = new HashMap<>();

        for (String tableLC : allTables) {
            String t = tableLC.toLowerCase();
            inDegree.put(t, 0);
            adjList.put(t, new ArrayList<>());
        }

        for (Map.Entry<String, List<String>> entry : graph.entrySet()) {
            String childLC = entry.getKey().toLowerCase();
            for (String parentLC : entry.getValue()) {
                if (!adjList.containsKey(parentLC)) {
                    continue;
                }
                adjList.get(parentLC).add(childLC);
                inDegree.put(childLC, inDegree.getOrDefault(childLC, 0) + 1);
            }
        }

        Queue<String> queue = new LinkedList<>();
        for (String tableLC : allTables) {
            String t = tableLC.toLowerCase();
            if (inDegree.getOrDefault(t, 0) == 0) {
                queue.add(t);
            }
        }

        List<String> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            sorted.add(current);

            for (String child : adjList.getOrDefault(current, List.of())) {
                inDegree.put(child, inDegree.get(child) - 1);
                if (inDegree.get(child) == 0) {
                    queue.add(child);
                }
            }
        }

        if (sorted.size() != allTables.size()) {
            throw new RuntimeException("Ciklicne zavisnosti u tabelama!");
        }

        return sorted;
    }
}
