package it.unipi.models;

import it.unipi.query.processor.QueryProcessor;
import it.unipi.utils.Constants;
import it.unipi.utils.DiskDataStructuresSearch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
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

        LexiconTerm lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch("test", queryProcessor.numberOfTerms, queryProcessor.lexiconBuffer);
        PostingListInterface postingList = new PostingListInterface(lexiconTerm, false);
        LinkedHashMap<Integer, SkipPointerEntry> skipPointers = postingList.getSkipPointers();
        for (Map.Entry<Integer, SkipPointerEntry> skipPointer : skipPointers.entrySet()) {
            int docId = skipPointer.getKey();
            postingList.nextGEQ(docId);
            Assertions.assertEquals(docId, postingList.getDocId());
        }
    }

    @Test
    void postingListTest() throws IOException {

        FileChannel lexiconChannel = FileChannel.open(Paths.get(Constants.LEXICON_FILE_PATH + "_test" + Constants.DAT_FORMAT));
        MappedByteBuffer lexiconBufferTest = lexiconChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexiconChannel.size()).load();

        for (int i = 0; i < queryProcessor.numberOfTerms; i++) {
            LexiconTerm lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch(i, queryProcessor.numberOfTerms, queryProcessor.lexiconBuffer);
            LexiconTerm lexiconTermTest = DiskDataStructuresSearch.lexiconDiskSearch(i, queryProcessor.numberOfTerms, lexiconBufferTest);
            Assertions.assertEquals(lexiconTerm.getTerm(), lexiconTermTest.getTerm());
            Assertions.assertEquals(lexiconTerm.documentFrequency, lexiconTermTest.documentFrequency);
            Assertions.assertEquals(lexiconTerm.collectionFrequency, lexiconTermTest.collectionFrequency);
            PostingListInterface postingList = new PostingListInterface(lexiconTerm, false);
            PostingListInterface postingListOld = new PostingListInterface(lexiconTermTest, true);
            while (postingListOld.nextOld()) {
                postingList.next();
                Assertions.assertEquals(postingListOld.getDocId(), postingList.getDocId());
                Assertions.assertEquals(postingListOld.getFreq(), postingList.getFreq());
            }
            postingList.closeList();
            postingListOld.closeList();
        }
    }
}
