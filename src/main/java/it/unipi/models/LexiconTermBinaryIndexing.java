package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public class LexiconTermBinaryIndexing extends LexiconTermIndexing {

    private byte[] encodedDocIDs;
    private byte[] encodedFrequencies;
    //used to keep pointers during merge
    static private int docIDsFileOffset = 0;
    static private int frequenciesFileOffset = 0;

    public LexiconTermBinaryIndexing() {
    }

    public LexiconTermBinaryIndexing(String term) {
        super(term);
    }

    public byte[] serialize() {
        return this.serializeBinary();
    }

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTermIndexing object
    public void deserialize(byte[] buffer) {
        deserializeBinary(buffer);
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

    public void writeToDisk(OutputStream docIDStream, OutputStream frequenciesStream, OutputStream lexiconStream) throws IOException {
        int numSkipBlocks;
        int blockSize;

        //(doc id,offset) list for skip pointers
        ArrayList<Integer> skipPointers = new ArrayList<>();

        if (this.documentFrequency > Constants.SKIP_POINTERS_THRESHOLD) {
            //decode posting list only if needed
            this.setPostingListDocIds(Utils.decode(this.encodedDocIDs));
            this.setPostingListFrequencies(Utils.decode(this.encodedFrequencies));

            //create sqrt(df) blocks of sqrt(df) size (rounded to the highest value when needed)
            blockSize = (int) Math.ceil(Math.sqrt(this.documentFrequency));
            numSkipBlocks = (int) Math.ceil((double)this.documentFrequency / (double)blockSize);

            int docIdOffset = 0;
            int frequencyOffset = 0;

            //Avoid inserting details about the last block, since they can be inferred from the previous ones
            for (int i = 0; i < numSkipBlocks - 1; i++) {
                // get first docID after the block
                int docId = postingListDocIds.get(blockSize * (i + 1));
                skipPointers.add(docId);
                // in subList from is inclusive, to is exclusive
                docIdOffset += Utils.getEncodingLength(this.getPostingListDocIds().subList((i * blockSize) , ((i + 1) * blockSize)));
                frequencyOffset += Utils.getEncodingLength(this.getPostingListFrequencies().subList((i * blockSize), ((i + 1) * blockSize)));
                skipPointers.add(docIdOffset);
                skipPointers.add(frequencyOffset);
            }
        }

        //set inverted file offsets for this term
        this.setDocIdsOffset(docIDsFileOffset);
        this.setFrequenciesOffset(frequenciesFileOffset);

        // docIDs
        if (skipPointers.size() > 0) {
            byte[] encodedDocIdsSkipPointers = Utils.intListToByteArray(skipPointers);
            docIDsFileOffset += encodedDocIdsSkipPointers.length;
            docIdsSize += encodedDocIdsSkipPointers.length;
            docIDStream.write(encodedDocIdsSkipPointers);
        }

        docIDsFileOffset += this.encodedDocIDs.length;
        docIdsSize += this.encodedDocIDs.length;
        docIDStream.write(this.encodedDocIDs);

        frequenciesFileOffset += this.encodedFrequencies.length;
        frequenciesSize += this.encodedFrequencies.length;
        frequenciesStream.write(this.encodedFrequencies);

        // lexicon
        lexiconStream.write(this.serialize());
    }
}
