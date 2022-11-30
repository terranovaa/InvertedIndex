package it.unipi;

public class Posting {
    private final int docID;
    private int frequency;

    public Posting(int docID, int frequency) {
        this.docID = docID;
        this.frequency = frequency;
    }

    public int getDocID() {
        return docID;
    }

    public int getFrequency() {
        return frequency;
    }

    public void increaseFrequency() {
        frequency++;
    }
}
