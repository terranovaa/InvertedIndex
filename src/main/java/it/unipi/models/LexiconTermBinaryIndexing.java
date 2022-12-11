package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class LexiconTermBinaryIndexing extends LexiconTermIndexing {

    public LexiconTermBinaryIndexing() {
    }

    public LexiconTermBinaryIndexing(String term) {
        super(term);
    }

    public byte[] serialize() {

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

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTermIndexing object
    public void deserialize(byte[] buffer) {
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

    public void writeToDisk(OutputStream docIDStream, OutputStream frequenciesStream, OutputStream lexiconStream) throws IOException {
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
        lexiconStream.write(this.serialize());
    }
}
