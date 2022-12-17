package it.unipi.queryProcessor;

import it.unipi.exceptions.IllegalQueryTypeException;
import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.Document;
import it.unipi.models.LexiconTerm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class QueryProcessorTests {

    QueryProcessor queryProcessor = new QueryProcessor();

    public QueryProcessorTests() throws IOException {
    }

    @Test
    void docTableDiskSearchTest() {
        Document document = queryProcessor.docTableDiskSearch(0);
        Assertions.assertEquals(document.getDocId(), 0);
    }

    @Test
    void lexiconDiskSearchTest() {
        LexiconTerm lexiconTerm = queryProcessor.lexiconDiskSearch("test");
        Assertions.assertEquals(lexiconTerm.getTerm(), "test");
    }

    @Test
    void processQueryTest() throws IllegalQueryTypeException, IOException, ExecutionException, TerminatedListException {
        long start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday");
        long end = System.currentTimeMillis();
        System.out.println(((double)(end - start)/1000) + " seconds");
        start = System.currentTimeMillis();
        queryProcessor.processQuery("I found out just yesterday");
        end = System.currentTimeMillis();
        System.out.println(((double)(end - start)/1000) + " seconds");
    }
}
