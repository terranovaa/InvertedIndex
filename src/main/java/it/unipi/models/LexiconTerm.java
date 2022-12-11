package it.unipi.models;

public class LexiconTerm {

    protected String term;
    //number of documents containing the term
    protected int documentFrequency;
    //number of total occurrences of the term
    protected int collectionFrequency;

    public LexiconTerm() {
        documentFrequency = 0;
        collectionFrequency = 0;
    }

    public LexiconTerm(String term) {
        super();
        this.term = term;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public int getDocumentFrequency() {
        return documentFrequency;
    }

    public int getCollectionFrequency() {
        return collectionFrequency;
    }

    public void setDocumentFrequency(int documentFrequency) {
        this.documentFrequency = documentFrequency;
    }

    public void setCollectionFrequency(int collectionFrequency) {
        this.collectionFrequency = collectionFrequency;
    }
}
