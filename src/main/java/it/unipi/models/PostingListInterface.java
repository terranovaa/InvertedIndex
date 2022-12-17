package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class PostingListInterface {

    // Not sure if necessary
    private final String term;
    private final FileChannel docIdsChannel;
    private final MappedByteBuffer docIdsBuffer;
    private final FileChannel freqChannel;
    private final MappedByteBuffer freqBuffer;
    private int currentDocID;
    private int currentFreq;
    private final int docIdsStartingOffset;

    private final LinkedHashMap<Integer, SkipPointerEntry> skipPointers;


    // NOTE I think we should use the constructor AS openList(), otherwise the FileChannels cannot be final.
    @SuppressWarnings("resource")
    public PostingListInterface(LexiconTerm lexiconTerm) throws IOException {
        term = lexiconTerm.getTerm();
        int docIdsSize = lexiconTerm.getDocIdsSize();
        int frequenciesSize = lexiconTerm.getFrequenciesSize();
        docIdsChannel = new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + Constants.DAT_FORMAT).getChannel();
        docIdsBuffer = docIdsChannel.map(FileChannel.MapMode.READ_ONLY, lexiconTerm.docIdsOffset, docIdsSize).load();
        freqChannel = new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + Constants.DAT_FORMAT).getChannel();
        freqBuffer = freqChannel.map(FileChannel.MapMode.READ_ONLY, lexiconTerm.frequenciesOffset, frequenciesSize).load();

        skipPointers = new LinkedHashMap<>();

        int documentFrequency = lexiconTerm.getDocumentFrequency();
        
        if (documentFrequency > Constants.SKIP_POINTERS_THRESHOLD) {
            int blockSize = (int) Math.ceil(Math.sqrt(documentFrequency));
            int numSkipBlocks = (int) Math.ceil((double)documentFrequency / (double)blockSize);
            int currentBlock = 0;
            while (docIdsBuffer.hasRemaining() && currentBlock < (numSkipBlocks - 1)) {
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

    private List<Byte> getNextInt(MappedByteBuffer list) {
        ArrayList<Byte> encodedInt = new ArrayList<>();
        // TODO should we just read a fixed number of bytes, maybe based on the total no of docs?
        byte buffer;
        do {
            buffer = list.get();
            encodedInt.add(buffer);
        } while ((buffer & 0xff) < 128);

        return encodedInt;
    }

    // returns false is we are at the end of the posting list, otherwise true
    public boolean next() {

        if (!docIdsBuffer.hasRemaining() || !freqBuffer.hasRemaining()) return false;

        List<Byte> encodedDocId = getNextInt(docIdsBuffer);
        currentDocID = Utils.decode(encodedDocId).get(0);

        List<Byte> encodedFreq = getNextInt(freqBuffer);
        currentFreq = Utils.decode(encodedFreq).get(0);

        return true;
    }
    
    public void nextGEQ(int docId) {

        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            // TODO need to check if it works
            // the second check is done in order to avoid going back to a lower offset (I think)
            if (skipPointer.getKey() < docId && skipPointer.getKey() > currentDocID) {
                docIdsBuffer.position(docIdsStartingOffset + (int) skipPointer.getValue().docIdOffset());
                freqBuffer.position((int) skipPointer.getValue().freqOffset());
            } else break;
        }

        while (currentDocID < docId) {
            next();
        }
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
}
