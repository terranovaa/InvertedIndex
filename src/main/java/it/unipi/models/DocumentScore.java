package it.unipi.models;

import javax.annotation.Nonnull;

public record DocumentScore(int docId, double score) implements Comparable<DocumentScore> {

    @Override
    public int compareTo(@Nonnull DocumentScore ds) {
        // TODO check if we need to swap the values
        return Double.compare(this.score, ds.score);
    }
}
