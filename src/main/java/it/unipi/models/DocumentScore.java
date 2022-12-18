package it.unipi.models;

import javax.annotation.Nonnull;

public record DocumentScore(int docId, double score) implements Comparable<DocumentScore> {

    @Override
    public int compareTo(@Nonnull DocumentScore ds) {
        return Double.compare(ds.score, this.score);
    }

    @Override
    public String toString(){
        return "" + this.docId + " score: " + this.score;
    }
}
