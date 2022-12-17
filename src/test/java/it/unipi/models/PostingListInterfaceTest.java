package it.unipi.models;

import it.unipi.queryProcessor.QueryProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class PostingListInterfaceTest {

    private QueryProcessor queryProcessor;

    @BeforeEach
    void setup() throws IOException {
        queryProcessor = new QueryProcessor();
    }

    @Test
    void skipBlocksTest() throws IOException {

        LexiconTerm lexiconTerm = queryProcessor.lexiconDiskSearch("test");
        PostingListInterface postingList = new PostingListInterface(lexiconTerm);
        LinkedHashMap<Integer, SkipPointerEntry> skipPointers = postingList.getSkipPointers();
        int docIdsStartingOffset = postingList.getDocIdsStartingOffset();
        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            int docId = skipPointer.getKey();
            long docIdOffset = skipPointer.getValue().docIdOffset();
            long freqOffset = skipPointer.getValue().freqOffset();
            postingList.setDocIdsBufferPosition(docIdsStartingOffset + (int) docIdOffset);
            postingList.setFreqBufferPosition((int) freqOffset);
            postingList.next();
            Assertions.assertEquals(docId, postingList.getDocId());
        }
    }
}
