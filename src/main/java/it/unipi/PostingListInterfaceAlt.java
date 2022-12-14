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

    private final FileChannel docIds;
    private final FileChannel frequencies;
    private int currentDocID;
    private int currentFreq;
    private int currentDocIdOffset;
    private int currentFreqOffset;
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
            docIds = docIdsStream.getChannel().position(lexiconTerm.getDocIdsOffset());
            frequencies = frequenciesStream.getChannel().position(lexiconTerm.getFrequenciesOffset());
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        docIdsSkipPointers = new LinkedHashMap<>();
        frequenciesSkipPointers = new LinkedHashMap<>();
        
        if (lexiconTerm.getDocumentFrequency() > Constants.SKIP_POINTERS_THRESHOLD) {
            // TODO load skip pointers in memory and update the current offsets
            //docIds.position(docIds.position() + size of the skip blocks)
        }
    }

    public int getDocId() {
        return currentDocID;
    }

    public int getFreq() {
        return currentFreq;
    }

    public void closeList() {
        try {
            docIds.close();
            frequencies.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Integer getNextInt(FileChannel channel) {
        ArrayList<Byte> encodedInt = new ArrayList<>();
        // TODO should we just read a fixed number of bytes, maybe based on the total no of docs?
        ByteBuffer buffer = ByteBuffer.allocate(1);
        try {
            do {
                channel.read(buffer);
            } while (buffer.hasRemaining() && (buffer.array()[0] & 0xff) < 128);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Utils.decode(encodedInt).get(0);
    }

    public void next() throws TerminatedListException, IOException {

        if (currentDocIdOffset >= docIdsSize || currentFreqOffset >= frequenciesSize)
            throw new TerminatedListException();
        
        currentDocID = getNextInt(docIds);
        currentFreq = getNextInt(frequencies);
    }
    
    public void nextGEQ(int docId) throws IOException, TerminatedListException {

        Iterator<Map.Entry<Integer, Integer>> docIdsSkipPointersIt = docIdsSkipPointers.entrySet().iterator();
        Iterator<Map.Entry<Integer, Integer>> freqSkipPointerIt = frequenciesSkipPointers.entrySet().iterator();

        while (docIdsSkipPointersIt.hasNext() && freqSkipPointerIt.hasNext()) {
            Map.Entry<Integer, Integer> docIdsSkipPointer = docIdsSkipPointersIt.next();
            Map.Entry<Integer, Integer> freqSkipPointer = freqSkipPointerIt.next();
            if (docIdsSkipPointer.getKey() < docId) {
                docIds.position(docIds.position() + docIdsSkipPointer.getValue());
                frequencies.position(frequencies.position() + freqSkipPointer.getValue());
            } else break;
        }

        while (currentDocID < docId) {
            next();
        }
    }

}
