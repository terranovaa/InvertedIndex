package it.unipi.models;

import java.util.SortedSet;
import java.util.TreeSet;

public class QueryCacheElement {

    private int frequency;
    private SortedSet<DocumentScore> docsPriorityQueue = new TreeSet<>();

    public QueryCacheElement(int frequency, SortedSet<DocumentScore> docsPriorityQueue) {
        this.frequency = frequency;
        this.docsPriorityQueue.addAll(docsPriorityQueue);
    }

    public int getFrequency() {
        return frequency;
    }

    public SortedSet<DocumentScore> getDocsPriorityQueue() {
        return docsPriorityQueue;
    }

    public void incrementFrequency() {
        frequency++;
    }
}
