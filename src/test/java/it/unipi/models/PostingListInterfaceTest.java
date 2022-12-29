package it.unipi.models;

import it.unipi.query.processor.QueryProcessor;
import it.unipi.utils.DiskDataStructuresSearch;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class PostingListInterfaceTest {

    private QueryProcessor queryProcessor;

    @BeforeEach
    void setup() throws IOException, ConfigurationException {
        queryProcessor = new QueryProcessor();
    }

    @Test
    void skipBlocksTest() throws IOException {

        LexiconTerm lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch("test", queryProcessor.numberOfTerms, queryProcessor.lexiconBuffer);
        assert lexiconTerm != null;
        PostingListInterface postingList = new PostingListInterface(lexiconTerm);
        LinkedHashMap<Integer, SkipPointerEntry> skipPointers = postingList.getSkipPointers();
        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            int docId = skipPointer.getKey();
            postingList.nextGEQ(docId);
            Assertions.assertEquals(docId, postingList.getDocId());
        }
    }
}
