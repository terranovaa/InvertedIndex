package it.unipi;

import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.LexiconTerm;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class PostingListInterfaceAlt {

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

    private final LinkedHashMap<Integer, Integer> docIdsSkipPointers;
    private final LinkedHashMap<Integer, Integer> frequenciesSkipPointers;


    // NOTE I think we should use the constructor AS openList(), otherwise the FileChannels cannot be final.
    public PostingListInterfaceAlt(LexiconTerm lexiconTerm) throws IOException {
        currentDocIdOffset = 0;
        currentFreqOffset = 0;
        docIdsSize = lexiconTerm.getDocIdsSize();
        frequenciesSize = lexiconTerm.getFrequenciesSize();
        try (FileInputStream docIdsStream = new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + Constants.DAT_FORMAT);
             FileInputStream frequenciesStream = new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + Constants.DAT_FORMAT)
        ){
            docIdsChannel = docIdsStream.getChannel().position(lexiconTerm.getDocIdsOffset());
            freqChannel = frequenciesStream.getChannel().position(lexiconTerm.getFrequenciesOffset());
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        docIdsSkipPointers = new LinkedHashMap<>();
        frequenciesSkipPointers = new LinkedHashMap<>();
        
        if (lexiconTerm.getDocumentFrequency() > Constants.SKIP_POINTERS_THRESHOLD) {
            // TODO load skip pointers in memory and update the current offsets
            //docIdsChannel.position(docIdsChannel.position() + size of the skip blocks)
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

    public void next() throws TerminatedListException, IOException {

        if (currentDocIdOffset >= docIdsSize || currentFreqOffset >= frequenciesSize)
            throw new TerminatedListException();

        List<Byte> encodedDocId = getNextInt(docIdsChannel);
        currentDocID = Utils.decode(encodedDocId).get(0);
        currentDocIdOffset += encodedDocId.size();

        List<Byte> encodedFreq = getNextInt(freqChannel);
        currentFreq = Utils.decode(encodedFreq).get(0);
        currentFreqOffset += encodedFreq.size();
    }
    
    public void nextGEQ(int docId) throws IOException, TerminatedListException {

        Iterator<Map.Entry<Integer, Integer>> docIdsSkipPointersIt = docIdsSkipPointers.entrySet().iterator();
        Iterator<Map.Entry<Integer, Integer>> freqSkipPointerIt = frequenciesSkipPointers.entrySet().iterator();

        while (docIdsSkipPointersIt.hasNext() && freqSkipPointerIt.hasNext()) {
            Map.Entry<Integer, Integer> docIdsSkipPointer = docIdsSkipPointersIt.next();
            Map.Entry<Integer, Integer> freqSkipPointer = freqSkipPointerIt.next();
            // TODO need to check if it works
            // the second check is done in order to avoid going back to a lower offset (I think)
            if (docIdsSkipPointer.getKey() < docId && docIdsSkipPointer.getKey() > currentDocID) {
                docIdsChannel.position(docIdsStartingOffset + docIdsSkipPointer.getValue());
                freqChannel.position(freqStartingOffset + freqSkipPointer.getValue());
            } else break;
        }

        while (currentDocID < docId) {
            next();
        }
    }

}
