package it.unipi;

import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.LexiconTerm;
import it.unipi.models.SkipPointerEntry;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class PostingListInterface {

    // Not sure if necessary
    private final String term;
    // TODO decide if we want to keep the FileChannel, or if we read all the posting list block into a ByteBuffer (let's compare the performance)
    private final FileChannel docIdsChannel;
    private final FileChannel freqChannel;
    private int currentDocID;
    private int currentFreq;
    private long currentDocIdOffset;
    private long currentFreqOffset;
    private final long docIdsStartingOffset;
    private final long freqStartingOffset;
    private final int docIdsSize;
    private final int frequenciesSize;

    private final LinkedHashMap<Integer, SkipPointerEntry> skipPointers;


    // NOTE I think we should use the constructor AS openList(), otherwise the FileChannels cannot be final.
    @SuppressWarnings("resource")
    public PostingListInterface(LexiconTerm lexiconTerm) throws IOException {
        term = lexiconTerm.getTerm();
        currentDocIdOffset = 0;
        currentFreqOffset = 0;
        docIdsSize = lexiconTerm.getDocIdsSize();
        frequenciesSize = lexiconTerm.getFrequenciesSize();
        docIdsChannel = new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + Constants.DAT_FORMAT).getChannel().position(lexiconTerm.getDocIdsOffset());
        freqChannel = new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + Constants.DAT_FORMAT).getChannel().position(lexiconTerm.getFrequenciesOffset());

        skipPointers = new LinkedHashMap<>();

        int documentFrequency = lexiconTerm.getDocumentFrequency();
        
        if (documentFrequency > Constants.SKIP_POINTERS_THRESHOLD) {
            int blockSize = (int) Math.ceil(Math.sqrt(documentFrequency));
            int numSkipBlocks = (int) Math.ceil((double)documentFrequency / (double)blockSize);
            ByteBuffer docIdsBuffer = ByteBuffer.allocate(numSkipBlocks * 12);
            docIdsChannel.read(docIdsBuffer);
            docIdsBuffer.position(0);
            while (docIdsBuffer.hasRemaining()) {
                int docId = docIdsBuffer.getInt();
                long docIdOffset = docIdsBuffer.getLong();
                long freqOffset = docIdsBuffer.getLong();
                skipPointers.put(docId, new SkipPointerEntry(docIdOffset, freqOffset));
            }
        }

        // Need to do it after reading the skip blocks because the offset in the skip pointers is relative to the start of the actual posting list
        docIdsStartingOffset = docIdsChannel.position();
        freqStartingOffset = freqChannel.position();
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

    public void closeList() {
        try {
            docIdsChannel.close();
            freqChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<Byte> getNextInt(FileChannel channel) {
        ArrayList<Byte> encodedInt = new ArrayList<>();
        // TODO should we just read a fixed number of bytes, maybe based on the total no of docs?
        ByteBuffer buffer = ByteBuffer.allocate(1);
        try {
            do {
                channel.read(buffer);
            } while ((buffer.array()[0] & 0xff) < 128);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return encodedInt;
    }

    // returns false is we are at the end of the posting list, otherwise true
    public boolean next() {

        if (currentDocIdOffset >= docIdsSize || currentFreqOffset >= frequenciesSize)
            return false;

        List<Byte> encodedDocId = getNextInt(docIdsChannel);
        currentDocID = Utils.decode(encodedDocId).get(0);
        currentDocIdOffset += encodedDocId.size();

        List<Byte> encodedFreq = getNextInt(freqChannel);
        currentFreq = Utils.decode(encodedFreq).get(0);
        currentFreqOffset += encodedFreq.size();

        return true;
    }
    
    public void nextGEQ(int docId) throws IOException {

        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            // TODO need to check if it works
            // the second check is done in order to avoid going back to a lower offset (I think)
            if (skipPointer.getKey() < docId && skipPointer.getKey() > currentDocID) {
                docIdsChannel.position(docIdsStartingOffset + skipPointer.getValue().docIdOffset());
                freqChannel.position(freqStartingOffset + skipPointer.getValue().freqOffset());
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
