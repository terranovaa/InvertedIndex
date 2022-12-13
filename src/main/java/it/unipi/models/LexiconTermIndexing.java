package it.unipi.models;

import java.util.ArrayList;

public class LexiconTermIndexing extends LexiconTerm {

    //posting_list of the term used during Indexing
    protected ArrayList<Integer> postingListDocIds = new ArrayList<>();
    protected ArrayList<Integer> postingListFrequencies = new ArrayList<>();
    //encoded posting_list used for performance during merge
    private int lastDocIdInserted;


    public ArrayList<Integer> getPostingListDocIds() {
        return postingListDocIds;
    }

    public ArrayList<Integer> getPostingListFrequencies() {
        return postingListFrequencies;
    }

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

    public void extendPostingList(ArrayList<Integer> docIDs, ArrayList<Integer> frequencies) {
        postingListDocIds.addAll(docIDs);
        postingListFrequencies.addAll(frequencies);
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
