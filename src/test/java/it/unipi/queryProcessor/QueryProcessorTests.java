package it.unipi.queryProcessor;

import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.Document;
import it.unipi.models.LexiconTerm;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.TextProcessingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

public class QueryProcessorTests {

    QueryProcessor queryProcessor = new QueryProcessor();

    public QueryProcessorTests() throws IOException {
    }

    @Test
    void docTableDiskSearchTest() {
        long start = System.currentTimeMillis();
        long end;
        Document document;

        for (int i = 0; i < 8_000_000; i++) {
            document = DiskDataStructuresSearch.docTableDiskSearch(i, queryProcessor.docTableBuffer);
            Assertions.assertEquals(document.getDocId(), i);
        }
        end = System.currentTimeMillis();
        System.out.println(((double)(end - start)/1000) + " seconds");
    }

    @Test
    void lexiconDiskSearchTest() {
        String[] words = new String[]{"test", "found", "party", "yesterday", "along", "cry"};
        ArrayList<String> stemmedWords = new ArrayList<>();
        for (String word: words) {
            stemmedWords.add(TextProcessingUtils.stemToken(word));
        }
        LexiconTerm lexiconTerm;
        long start = System.currentTimeMillis();
        for (String word: stemmedWords) {
            lexiconTerm = DiskDataStructuresSearch.lexiconDiskSearch(word, queryProcessor.numberOfTerms, queryProcessor.lexiconBuffer);
            Assertions.assertEquals(lexiconTerm.getTerm(), word);
        }
        long end = System.currentTimeMillis();
        System.out.println(((double)(end - start)/1000) + " seconds");
    }

    @Test
    void processQueryTest() throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException {
        long start;
        long end;
        start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday");
        end = System.currentTimeMillis();
        System.out.println(((double) (end - start) / 1000) + " seconds");
        start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday");
        end = System.currentTimeMillis();
        System.out.println(((double) (end - start) / 1000) + " seconds");

        start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday you had some problems during winter probably lexicon awards");
        end = System.currentTimeMillis();
        System.out.println(((double) (end - start) / 1000) + " seconds");
        start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday you had some problems during winter probably lexicon awards");
        end = System.currentTimeMillis();
        System.out.println(((double) (end - start) / 1000) + " seconds");
    }
}
