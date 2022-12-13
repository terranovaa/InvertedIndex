package it.unipi.models;

abstract class LexiconTerm {

    protected String term;
    //number of documents containing the term
    protected int documentFrequency;
    //number of total occurrences of the term
    protected int collectionFrequency;

    protected int docIdsOffset;
    protected int frequenciesOffset;
    protected int docIdsSize;
    protected int frequenciesSize;

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

    public void setDocIdsOffset(int docIdsOffset) {
        this.docIdsOffset = docIdsOffset;
    }

    public int getDocIdsOffset() {
        return docIdsOffset;
    }

    public void setFrequenciesOffset(int frequenciesOffset) {
        this.frequenciesOffset = frequenciesOffset;
    }

    public int getFrequenciesOffset() {
        return frequenciesOffset;
    }

    public int getDocIdsSize() {
        return docIdsSize;
    }

    public void setDocIdsSize(int docIdsSize) {
        this.docIdsSize = docIdsSize;
    }

    public int getFrequenciesSize() {
        return frequenciesSize;
    }

    public void setFrequenciesSize(int frequenciesSize) {
        this.frequenciesSize = frequenciesSize;
    }
}
