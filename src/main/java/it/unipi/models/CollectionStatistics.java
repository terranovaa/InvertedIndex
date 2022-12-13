package it.unipi.models;

public class CollectionStatistics {

    private int numDocs;
    private int numDistinctTerms;
    private int numTotalTerms;

    public CollectionStatistics() {
        numDocs = 0;
        numDistinctTerms = 0;
        numTotalTerms = 0;
    }

    public CollectionStatistics(int numDocs, int numDistinctTerms, int numTotalTerms) {
        this.numDocs = numDocs;
        this.numDistinctTerms = numDistinctTerms;
        this.numTotalTerms = numTotalTerms;
    }

    public int getNumDocs() {
        return numDocs;
    }

    public void setNumDocs(int numDocs) {
        this.numDocs = numDocs;
    }

    public int getNumDistinctTerms() {
        return numDistinctTerms;
    }

    public void setNumDistinctTerms(int numDistinctTerms) {
        this.numDistinctTerms = numDistinctTerms;
    }

    public int getNumTotalTerms() {
        return numTotalTerms;
    }

    public void setNumTotalTerms(int numTotalTerms) {
        this.numTotalTerms = numTotalTerms;
    }

    public void incrementNumDistinctTerms() {
        numDistinctTerms++;
    }

    public void incrementNumTotalTerms() {
        numTotalTerms++;
    }
}
