package it.unipi.models;

import javax.annotation.Nonnull;

// immutable class used for storing the BM25 score of a document during query processing
public record DocumentScore(String docNo, double score) implements Comparable<DocumentScore> {

    @Override
    public int compareTo(@Nonnull DocumentScore ds) {
        return Double.compare(ds.score, this.score);
    }

    @Override
    public String toString(){
        return "" + this.docNo + " score: " + this.score;
    }
}
