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

    //DEBUG
    public void printInfo(){
        System.out.println("term: " + term +
                " | df: " + documentFrequency +
                " | cf: " + collectionFrequency);
        for(Posting posting: postingList){
            System.out.print("[" + "docID: " + posting.getDocID() + " | freq: " + posting.getFrequency() + "] ");
        }
        System.out.println("\n------------------------------");
    }
}
