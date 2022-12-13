package it.unipi.models;

import java.util.ArrayList;

public class LexiconTermIndexing extends LexiconTerm {

    //posting_list of the term used during Indexing
    protected ArrayList<Integer> postingListDocIds = new ArrayList<>();
    protected ArrayList<Integer> postingListFrequencies = new ArrayList<>();
    //encoded posting_list used for performance during merge
    protected byte[] encodedDocIDs;
    protected byte[] encodedFrequencies;
    private int lastDocIdInserted;

    //used to keep pointers during merge
    static protected int docIDsFileOffset = 0;
    static protected int frequenciesFileOffset = 0;

    public void setPostingListDocIds(ArrayList<Integer> postingListDocIds) {
        this.postingListDocIds = postingListDocIds;
    }

    public void setPostingListFrequencies(ArrayList<Integer> postingListFrequencies) {
        this.postingListFrequencies = postingListFrequencies;
    }

    public LexiconTermIndexing() {
        super();
        lastDocIdInserted = -1;
    }

    public LexiconTermIndexing(String term) {
        super(term);
        lastDocIdInserted = -1;
    }

    public void addToPostingList(int docID) {
        //increase total occurrences by 1
        collectionFrequency++;
        if(lastDocIdInserted != docID){
            //first occurrence in current document
            documentFrequency++;
            lastDocIdInserted = docID;
            postingListDocIds.add(docID);
            postingListFrequencies.add(1);
        }
        else{
            //additional occurrence for the previous document
            Integer frequency = postingListFrequencies.get(postingListFrequencies.size() - 1);
            postingListFrequencies.set(postingListFrequencies.size() - 1, frequency + 1);
        }
    }

    public ArrayList<Integer> getPostingListDocIds() {
        return postingListDocIds;
    }

    public ArrayList<Integer> getPostingListFrequencies() {
        return postingListFrequencies;
    }


    public void extendPostingList(ArrayList<Integer> docIDs, ArrayList<Integer> frequencies) {
        postingListDocIds.addAll(docIDs);
        postingListFrequencies.addAll(frequencies);
    }

    public void mergeEncodedPostings(byte[] encodedDocIDs, byte[] encodedFrequencies){
        if(this.encodedDocIDs == null){
            this.encodedDocIDs = encodedDocIDs;
            this.encodedFrequencies = encodedFrequencies;
        } else {
            //doc_ids
            byte[] mergedDocIDsArray = new byte[this.encodedDocIDs.length + encodedDocIDs.length];
            System.arraycopy(this.encodedDocIDs, 0, mergedDocIDsArray, 0, this.encodedDocIDs.length);
            System.arraycopy(encodedDocIDs, 0, mergedDocIDsArray, this.encodedDocIDs.length, encodedDocIDs.length);
            this.encodedDocIDs = mergedDocIDsArray;

            //frequencies
            byte[] mergedFrequenciesArray = new byte[this.encodedFrequencies.length + encodedFrequencies.length];
            System.arraycopy(this.encodedFrequencies, 0, mergedFrequenciesArray, 0, this.encodedFrequencies.length);
            System.arraycopy(encodedFrequencies, 0, mergedFrequenciesArray, this.encodedFrequencies.length, encodedFrequencies.length);
            this.encodedFrequencies = mergedFrequenciesArray;
        }
    }

    //DEBUG
    public void printInfo(int numPostings){
        System.out.println("term: " + term +
                " | df: " + documentFrequency +
                " | cf: " + collectionFrequency +
                " | dOffset: " + docIdsOffset +
                " | fOffset: " + frequenciesOffset +
                " | dSize: " + docIdsSize +
                " | fSize: " + frequenciesSize);
        for(int i = 0; i < numPostings; i++){
            System.out.print("[" + "docID: " + postingListDocIds.get(i) + " | freq: " + postingListFrequencies.get(i) + "] ");
        }
        System.out.println("\n------------------------------");
    }
}
