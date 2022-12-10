package it.unipi;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LexiconTerm {
    // TODO this field takes up space in memory, maybe we can remove it?
    private String term;
    //number of documents containing the term
    private int documentFrequency;
    //number of total occurrences of the term
    private int collectionFrequency;
    //posting_list of the term during indexing
    private ArrayList<Integer> postingListDocIds = new ArrayList<>();
    private ArrayList<Integer> postingListFrequencies = new ArrayList<>();
    //encoded posting_list used for performance during merge
    private byte[] encodedDocIDs;
    private byte[] encodedFrequencies;
    private int lastDocIdInserted;

    private int docIdsOffset;
    private int frequenciesOffset;
    private int docIdsSize;
    private int frequenciesSize;

    //used to keep pointers during merge
    static private int docIDsFileOffset = 0;
    static private int frequenciesFileOffset = 0;

    public byte[] getEncodedDocIDs() {
        return encodedDocIDs;
    }

    public void setEncodedDocIDs(byte[] encodedDocIDs) {
        this.encodedDocIDs = encodedDocIDs;
    }

    public byte[] getEncodedFrequencies() {
        return encodedFrequencies;
    }

    public void setEncodedFrequencies(byte[] encodedFrequencies) {
        this.encodedFrequencies = encodedFrequencies;
    }

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

    public int getLastDocIdInserted() {
        return lastDocIdInserted;
    }

    public void setLastDocIdInserted(int lastDocIdInserted) {
        this.lastDocIdInserted = lastDocIdInserted;
    }

    public void setPostingListDocIds(ArrayList<Integer> postingListDocIds) {
        this.postingListDocIds = postingListDocIds;
    }

    public void setPostingListFrequencies(ArrayList<Integer> postingListFrequencies) {
        this.postingListFrequencies = postingListFrequencies;
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



    public void addPosting(int docID, int frequency){
        postingListDocIds.add(docID);
        postingListFrequencies.add(frequency);
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
            //docids
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
    public void printInfo(){
        System.out.println("term: " + term +
                " | df: " + documentFrequency +
                " | cf: " + collectionFrequency +
                " | dOffset: " + docIdsOffset +
                " | fOffset: " + frequenciesOffset +
                " | dSize: " + docIdsSize +
                " | fSize: " + frequenciesSize);
        for(int i = 0; i < postingListDocIds.size(); i++){
            System.out.print("[" + "docID: " + postingListDocIds.get(i) + " | freq: " + postingListFrequencies.get(i) + "] ");
        }
        System.out.println("\n------------------------------");
    }

     byte[] serializeBinary() {

        byte[] lexiconEntry = new byte[Constants.LEXICON_ENTRY_SIZE];
        //variable number of bytes
        byte[] entryTerm = term.getBytes(StandardCharsets.UTF_8);
        //fixed number of bytes, 4 for each integer
        byte[] entryDf = Utils.intToByteArray(documentFrequency);
        byte[] entryCf = Utils.intToByteArray(collectionFrequency);
        byte[] entryDocIDOffset = Utils.intToByteArray(docIdsOffset);
        byte[] entryFrequenciesOffset = Utils.intToByteArray(frequenciesOffset);
        byte[] entryDocIDSize = Utils.intToByteArray(docIdsSize);
        byte[] entryFrequenciesSize = Utils.intToByteArray(frequenciesSize);
        try {
            //fill the first part of the buffer with the utf-8 representation of the term, leave the rest to 0
            System.arraycopy(entryTerm, 0, lexiconEntry, 0, entryTerm.length);
            //fill the last part of the buffer with statistics and offsets
            System.arraycopy(entryDf, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 24, 4);
            System.arraycopy(entryCf, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 20, 4);
            System.arraycopy(entryDocIDOffset, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 16, 4);
            System.arraycopy(entryFrequenciesOffset, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 12, 4);
            System.arraycopy(entryDocIDSize, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 8, 4);
            System.arraycopy(entryFrequenciesSize, 0, lexiconEntry, Constants.LEXICON_ENTRY_SIZE - 4, 4);
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
         return lexiconEntry;
    }

    public String[] serializeTextual() {
        ArrayList<String> list = new ArrayList<>();
        list.add(term);
        list.add(Integer.toString(documentFrequency));
        list.add(Integer.toString(collectionFrequency));
        return list.toArray(new String[0]);
    }

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTerm object
     void deserializeBinary(byte[] buffer) {
        //to decode the term, detect the position of the first byte equal 0
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        term = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        documentFrequency = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 24);
        collectionFrequency = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 20);
        docIdsOffset = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 16);
        frequenciesOffset = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 12);
        docIdsSize = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 8);
        frequenciesSize = Utils.byteArrayToInt(buffer, Constants.LEXICON_ENTRY_SIZE - 4);
    }

    void deserializeTextual(String buffer) {
        List<String> elements = Arrays.asList(buffer.split(","));
        term = elements.get(0);
        documentFrequency = Integer.parseInt(elements.get(1));
        collectionFrequency = Integer.parseInt(elements.get(2));
    }

    void writeToDiskBinary(OutputStream docIDStream, OutputStream frequenciesStream, OutputStream lexiconStream) throws IOException {
        int numSkipBlocks;
        ArrayList<Integer> docIdsSkipPointers = new ArrayList<>();
        ArrayList<Integer> frequenciesSkipPointers = new ArrayList<>();
        if ((numSkipBlocks = (int) Math.floor((documentFrequency - 1) / (double) Constants.NUM_POSTINGS_PER_BLOCK)) > 0) {
            this.setPostingListDocIds(Utils.decode(this.encodedDocIDs));
            this.setPostingListFrequencies(Utils.decode(this.encodedFrequencies));

            for (int i = 0; i < numSkipBlocks; i++) {
                // First element of the block
                int docId = postingListDocIds.get(Constants.NUM_POSTINGS_PER_BLOCK * (i + 1));
                int frequency = postingListFrequencies.get(Constants.NUM_POSTINGS_PER_BLOCK * (i + 1));
                docIdsSkipPointers.add(docId);
                frequenciesSkipPointers.add(frequency);
                // from is inclusive, to is exclusive
                int docIdOffset = Utils.getEncodingLength(this.getPostingListDocIds().subList((i * Constants.NUM_POSTINGS_PER_BLOCK) , ((i + 1) * Constants.NUM_POSTINGS_PER_BLOCK)));
                int frequencyOffset = Utils.getEncodingLength(this.getPostingListFrequencies().subList((i * Constants.NUM_POSTINGS_PER_BLOCK), ((i + 1) * Constants.NUM_POSTINGS_PER_BLOCK)));
                docIdsSkipPointers.add(docIdOffset);
                frequenciesSkipPointers.add(frequencyOffset);
            }
        }

        this.setDocIdsOffset(docIDsFileOffset);
        this.setFrequenciesOffset(frequenciesFileOffset);
        // docIDs
        if (docIdsSkipPointers.size() > 0) {
            byte[] encodedDocIdsSkipPointers = Utils.encode(docIdsSkipPointers);
            docIDsFileOffset += encodedDocIdsSkipPointers.length;
            docIdsSize += encodedDocIdsSkipPointers.length;
            docIDStream.write(encodedDocIdsSkipPointers);
        }
        //byte[] encodedDocIDs = Utils.encode(this.getPostingListDocIds());
        docIDsFileOffset += this.encodedDocIDs.length;
        docIdsSize += this.encodedDocIDs.length;
        docIDStream.write(this.encodedDocIDs);
        // frequencies
        if (frequenciesSkipPointers.size() > 0) {
            byte[] encodedFrequenciesSkipPointers = Utils.encode(frequenciesSkipPointers);
            frequenciesFileOffset += encodedFrequenciesSkipPointers.length;
            frequenciesSize += encodedFrequenciesSkipPointers.length;
            frequenciesStream.write(encodedFrequenciesSkipPointers);
        }
        //byte[] encodedFrequencies = Utils.encode(this.getPostingListFrequencies());
        frequenciesFileOffset += this.encodedFrequencies.length;
        frequenciesSize += this.encodedFrequencies.length;
        frequenciesStream.write(this.encodedFrequencies);
        // lexicon
        lexiconStream.write(this.serializeBinary());
    }

    void writeToDiskTextual(BufferedWriter docIDStream, BufferedWriter frequenciesStream, BufferedWriter lexiconStream) throws IOException {
        //docIDs
        List<Integer> docIDs = this.getPostingListDocIds();
        for(int i = 0; i < docIDs.size(); ++i)
            if(i != docIDs.size()-1)
                docIDStream.write(docIDs.get(i).toString()+",");
            else docIDStream.write(docIDs.get(i).toString()+"\n");

        // frequencies
        List<Integer> frequencies = this.getPostingListFrequencies();
        for(int i = 0; i < frequencies.size(); ++i)
            if(i != frequencies.size()-1)
                frequenciesStream.write(frequencies.get(i).toString()+",");
            else frequenciesStream.write(frequencies.get(i).toString()+"\n");

        //lexicon term
        String[] lexiconEntry = this.serializeTextual();
        for(int i = 0; i < lexiconEntry.length; ++i)
            if(i != lexiconEntry.length-1)
                lexiconStream.write(lexiconEntry[i]+",");
            else lexiconStream.write(lexiconEntry[i]+"\n");
    }
}
