package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.EncodingUtils;

import javax.annotation.Nonnull;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class PostingListInterface implements Comparable<PostingListInterface> {

    private final String term;
    private final FileChannel docIdsChannel;
    // memory mapped buffer of the posting list portion relative to the term
    private final MappedByteBuffer docIdsBuffer;
    private final FileChannel freqChannel;
    // memory mapped buffer of the posting list portion relative to the term
    private final MappedByteBuffer freqBuffer;
    private int currentDocID;
    private int previousDocID;
    private int currentFreq;
    // useful if there are skip blocks
    private final int docIdsStartingOffset;
    // statistics for MaxScore
    private final double termUpperBound;
    // skip blocks
    private final LinkedHashMap<Integer, SkipPointerEntry> skipPointers;


    // the constructor corresponds to openList() (otherwise FileChannels could not be final)
    @SuppressWarnings("resource")
    public PostingListInterface(LexiconTerm lexiconTerm) throws IOException {
        term = lexiconTerm.getTerm();
        termUpperBound = lexiconTerm.termUpperBound;
        int docIdsSize = lexiconTerm.getDocIdsSize();
        int frequenciesSize = lexiconTerm.getFrequenciesSize();
        docIdsChannel = new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH +"NEW"+ Constants.DAT_FORMAT).getChannel();
        docIdsBuffer = docIdsChannel.map(FileChannel.MapMode.READ_ONLY, lexiconTerm.docIdsOffset, docIdsSize).load();
        freqChannel = new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + Constants.DAT_FORMAT).getChannel();
        freqBuffer = freqChannel.map(FileChannel.MapMode.READ_ONLY, lexiconTerm.frequenciesOffset, frequenciesSize).load();
        previousDocID = -1;
        skipPointers = new LinkedHashMap<>();

        int documentFrequency = lexiconTerm.getDocumentFrequency();

        // checking if the term has skip pointers
        if (documentFrequency > Constants.SKIP_POINTERS_THRESHOLD) {
            // number of skip blocks if root of document freq
            int blockSize = (int) Math.ceil(Math.sqrt(documentFrequency));
            int numSkipBlocks = (int) Math.ceil((double)documentFrequency / (double)blockSize);
            int currentBlock = 0;
            // retrieving all the skip blocks
            while (currentBlock < (numSkipBlocks - 1)) {
                int docId = docIdsBuffer.getInt();
                long docIdOffset = docIdsBuffer.getLong();
                long freqOffset = docIdsBuffer.getLong();
                skipPointers.put(docId, new SkipPointerEntry(docIdOffset, freqOffset));
                currentBlock++;
            }
        }

        // Need to do it after reading the skip blocks because the offset in the skip pointers is relative to the start of the actual posting list
        docIdsStartingOffset = docIdsBuffer.position();
    }

    public int getDocId() {
        return currentDocID;
    }

    public int getFreq() {
        return currentFreq;
    }

    public String getTerm() {
        return term;
    }

    public int getDocIdsStartingOffset() {
        return docIdsStartingOffset;
    }

    public LinkedHashMap<Integer, SkipPointerEntry> getSkipPointers() {
        return skipPointers;
    }

    public void setDocIdsBufferPosition (int position) {
        docIdsBuffer.position(position);
    }

    public void setFreqBufferPosition (int position) {
        freqBuffer.position(position);
    }

    public double getTermUpperBound() {
        return termUpperBound;
    }

    public void closeList() {
        try {
            docIdsBuffer.clear();
            docIdsChannel.close();
            freqBuffer.clear();
            freqChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // returns false is we are at the end of the posting list, otherwise true
    // moves forward by one doc in the posting lists
    public boolean next() {

        if (!docIdsBuffer.hasRemaining() || !freqBuffer.hasRemaining()) return false;

        List<Byte> encodedDocId = getNextInt(docIdsBuffer);
        currentDocID = EncodingUtils.decode(encodedDocId).get(0);

        // gaps
        if(previousDocID != -1)
            currentDocID += previousDocID;

        previousDocID = currentDocID;

        List<Byte> encodedFreq = getNextInt(freqBuffer);
        currentFreq = EncodingUtils.decode(encodedFreq).get(0);

        return true;
    }

    // decodes a VariableByte encoded int from the posting list
    private List<Byte> getNextInt(MappedByteBuffer list) {
        ArrayList<Byte> encodedInt = new ArrayList<>();
        byte buffer;
        do {
            buffer = list.get();
            encodedInt.add(buffer);
        } while ((buffer & 0xff) < 128); // last digit of a VariableByte encoded int is 1xxxxxxx

        return encodedInt;
    }

    // returns false is we are at the end of the posting list, otherwise true
    // moves to a doc_id greater or equal than docId (if it exists)
    public boolean nextGEQ(int docId) {
        if (!docIdsBuffer.hasRemaining() || !freqBuffer.hasRemaining()) return false;

        // current docId is already GEQ than docId, no need to do anything
        if (currentDocID >= docId) return true;

        int startingDocId = currentDocID;

        int skipDocId = 0;
        int skipDocIdOffset = 0;

        boolean postingListHasNext = true;

        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            if (skipPointer.getKey() < startingDocId) continue; // currentDocId is already GEQ than the docId of the skip pointer
            if (skipPointer.getKey() <= docId) {
                skipDocId = skipPointer.getKey();
                skipDocIdOffset = (int) skipPointer.getValue().docIdOffset();
            } else break; // we reached a skipDocId greater than docId
        }

        // we move the buffers to the skip offsets
        if (skipDocId > startingDocId) {
            docIdsBuffer.position(docIdsStartingOffset + skipDocIdOffset);
            next();
        }

        // we use next() until we reach a doc GEQ than docId
        while (currentDocID < docId && postingListHasNext) {
            postingListHasNext = next();
        }

        return postingListHasNext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostingListInterface that = (PostingListInterface) o;
        return term.equals(that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term);
    }

    // used for ordering the list by increasing term upper bound
    @Override
    public int compareTo(@Nonnull PostingListInterface pli) {
        return Double.compare(this.termUpperBound, pli.termUpperBound);
    }
}
