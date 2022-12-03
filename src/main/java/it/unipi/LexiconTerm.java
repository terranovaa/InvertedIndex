package it.unipi;

import java.util.ArrayList;

public class LexiconTerm {
    //probably not needed
    private final String term;
    //number of documents containing the term
    private int documentFrequency;
    //number of total occurrences of the term
    private int collectionFrequency;
    //posting_list of the term during indexing
    private final ArrayList<Posting> postingList;
    //useful for checking if a docid already belongs to the posting
    private int lastDocIDInserted;

    private int docIDsOffset;
    private int frequenciesOffset;
    private int docIDsSize;
    private int frequenciesSize;

    public int getDocIDsOffset() {
        return docIDsOffset;
    }

    public void setDocIDsOffset(int docIDsOffset) {
        this.docIDsOffset = docIDsOffset;
    }

    public int getFrequenciesOffset() {
        return frequenciesOffset;
    }

    public void setFrequenciesOffset(int frequenciesOffset) {
        this.frequenciesOffset = frequenciesOffset;
    }

    public int getDocIDsSize() {
        return docIDsSize;
    }

    public void setDocIDsSize(int docIDsSize) {
        this.docIDsSize = docIDsSize;
    }

    public int getFrequenciesSize() {
        return frequenciesSize;
    }

    public void setFrequenciesSize(int frequenciesSize) {
        this.frequenciesSize = frequenciesSize;
    }

    public String getTerm() {
        return term;
    }

    public int getDocumentFrequency() {
        return documentFrequency;
    }

    public void setDocumentFrequency(int documentFrequency) {
        this.documentFrequency = documentFrequency;
    }

    public int getCollectionFrequency() {
        return collectionFrequency;
    }

    public void setCollectionFrequency(int collectionFrequency) {
        this.collectionFrequency = collectionFrequency;
    }

    public ArrayList<Posting> getPostingList() {
        return postingList;
    }

    public int getLastDocIDInserted() {
        return lastDocIDInserted;
    }

    public void setLastDocIDInserted(int lastDocIDInserted) {
        this.lastDocIDInserted = lastDocIDInserted;
    }

    public LexiconTerm(String term) {
        this.term = term;
        documentFrequency = 0;
        collectionFrequency = 0;
        lastDocIDInserted = -1;
        postingList = new ArrayList<>();
    }

    public void addToPostingList(int docID) {
        //increase total occurrences by 1
        collectionFrequency++;
        if(lastDocIDInserted != docID){
            //first occurrence in current document
            documentFrequency++;
            lastDocIDInserted = docID;
            postingList.add(new Posting(docID, 1));
        }
        else{
            //additional occurrence for the previous document
            Posting docPosting = postingList.get(postingList.size() - 1);
            docPosting.increaseFrequency();
        }
    }

    public void addPosting(int docID, int frequency){
        postingList.add(new Posting(docID, frequency));
    }

    //DEBUG
    public void printInfo(){
        System.out.println("term: " + term +
                " | df: " + documentFrequency +
                " | cf: " + collectionFrequency +
                " | dOffset: " + docIDsOffset +
                " | fOffset: " + frequenciesOffset +
                " | dSize: " + docIDsSize +
                " | fSize: " + frequenciesSize);
        for(Posting posting: postingList){
            System.out.print("[" + "docID: " + posting.getDocID() + " | freq: " + posting.getFrequency() + "] ");
        }
        System.out.println("\n------------------------------");
    }
}
