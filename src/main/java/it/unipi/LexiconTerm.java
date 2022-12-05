package it.unipi;

import it.unipi.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class LexiconTerm {
    // TODO this field takes up space in memory, maybe we can remove it?
    private String term;
    //number of documents containing the term
    private int documentFrequency;
    //number of total occurrences of the term
    private int collectionFrequency;
    //posting_list of the term during indexing
    private final ArrayList<Posting> postingList = new ArrayList<>();
    //useful for checking if a docid already belongs to the posting
    private final ArrayList<Integer> postingListDocIds = new ArrayList<>();
    private final ArrayList<Integer> postingListFrequencies = new ArrayList<>();
    private int lastDocIdInserted;

    private int docIdsOffset;
    private int frequenciesOffset;
    private int docIdsSize;
    private int frequenciesSize;

    public int getDocIdsOffset() {
        return docIdsOffset;
    }

    public void setDocIdsOffset(int docIdsOffset) {
        this.docIdsOffset = docIdsOffset;
    }

    public int getFrequenciesOffset() {
        return frequenciesOffset;
    }

    public void setFrequenciesOffset(int frequenciesOffset) {
        this.frequenciesOffset = frequenciesOffset;
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

    public int getLastDocIdInserted() {
        return lastDocIdInserted;
    }

    public void setLastDocIdInserted(int lastDocIdInserted) {
        this.lastDocIdInserted = lastDocIdInserted;
    }

    public LexiconTerm() {
    }

    public LexiconTerm(String term) {
        this.term = term;
        documentFrequency = 0;
        collectionFrequency = 0;
        lastDocIdInserted = -1;
    }

    public void addToPostingList(int docID) {
        //increase total occurrences by 1
        collectionFrequency++;
        if(lastDocIdInserted != docID){
            //first occurrence in current document
            documentFrequency++;
            lastDocIdInserted = docID;
            postingList.add(new Posting(docID, 1));
            postingListDocIds.add(docID);
            postingListFrequencies.add(1);
        }
        else{
            //additional occurrence for the previous document
            Posting docPosting = postingList.get(postingList.size() - 1);
            docPosting.increaseFrequency();
            Integer frequency = postingListFrequencies.get(postingList.size() - 1);
            postingListFrequencies.set(postingList.size() - 1, frequency + 1);
        }
    }

    public ArrayList<Integer> getPostingListDocIds() {
        return postingListDocIds;
    }

    public ArrayList<Integer> getPostingListFrequencies() {
        return postingListFrequencies;
    }

    public void addPosting(int docID, int frequency){
        postingList.add(new Posting(docID, frequency));
        postingListDocIds.add(docID);
        postingListFrequencies.add(frequency);
    }

    //DEBUG
    public void printInfo(){
        System.out.println("term: " + term +
                " | df: " + documentFrequency +
                " | cf: " + collectionFrequency +
                " | dOffset: " + docIdsOffset +
                " | fOffset: " + frequenciesOffset +
                " | dSize: " + docIdsSize +
                " | fSize: " + frequenciesSize);
        for(Posting posting: postingList){
            System.out.print("[" + "docID: " + posting.getDocID() + " | freq: " + posting.getFrequency() + "] ");
        }
        System.out.println("\n------------------------------");
    }

     byte[] serialize() {

        final int LEXICON_ENTRY_SIZE = 144;

        byte[] lexiconEntry = new byte[LEXICON_ENTRY_SIZE];
        //variable number of bytes
        byte[] entryTerm = term.getBytes(StandardCharsets.UTF_8);
        //fixed number of bytes, 4 for each integer
        byte[] entryDf = Utils.intToByteArray(documentFrequency);
        byte[] entryCf = Utils.intToByteArray(collectionFrequency);
        byte[] entryDocIDOffset = Utils.intToByteArray(docIdsOffset);
        byte[] entryFrequenciesOffset = Utils.intToByteArray(frequenciesOffset);
        byte[] entryDocIDSize = Utils.intToByteArray(docIdsSize);
        byte[] entryFrequenciesSize = Utils.intToByteArray(frequenciesSize);
        //fill the first part of the buffer with the utf-8 representation of the term, leave the rest to 0
        System.arraycopy(entryTerm, 0, lexiconEntry, 0, entryTerm.length);
        //fill the last part of the buffer with statistics and offsets
        System.arraycopy(entryDf, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 24, 4);
        System.arraycopy(entryCf, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 20, 4);
        System.arraycopy(entryDocIDOffset, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 16, 4);
        System.arraycopy(entryFrequenciesOffset, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 12, 4);
        System.arraycopy(entryDocIDSize, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 8, 4);
        System.arraycopy(entryFrequenciesSize, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 4, 4);
        return lexiconEntry;
    }

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTerm object
     void deserialize(byte[] buffer) {
        final int LEXICON_ENTRY_SIZE = 144;
        //to decode the term, detect the position of the first byte equal 0
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        term = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        documentFrequency = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 24);
        collectionFrequency = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 20);
        docIdsOffset = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 16);
        frequenciesOffset = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 12);
        docIdsSize = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 8);
        frequenciesSize = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 4);
    }
}
