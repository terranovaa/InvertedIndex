package it.unipi.models;

import javax.annotation.Nonnull;

public record DocumentScore(int docId, int score) implements Comparable<DocumentScore> {

    @Override
    public int compareTo(@Nonnull DocumentScore ds) {
        return ds.score - this.score;
    }
}
