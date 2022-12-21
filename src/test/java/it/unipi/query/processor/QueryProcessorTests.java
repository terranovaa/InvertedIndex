package it.unipi.query.processor;

import it.unipi.models.Document;
import it.unipi.models.DocumentScore;
import it.unipi.models.LexiconTerm;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.TextProcessingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

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
            assert lexiconTerm != null;
            Assertions.assertEquals(lexiconTerm.getTerm(), word);
        }
        long end = System.currentTimeMillis();
        System.out.println(((double)(end - start)/1000) + " seconds");
    }

    @Test
    void runQueryTest() {

        int k = 10;

        queryProcessor.runQuery("AND I found out just yesterday", k);
        queryProcessor.runQuery("AND I found out just yesterday", k);
        queryProcessor.runQuery("OR I found out just yesterday", k);
        queryProcessor.runQuery("OR I found out just yesterday", k);
        queryProcessor.runQuery("OR I found out just yesterday you had some problems during winter probably lexicon awards", k);
        queryProcessor.runQuery("OR I found out just yesterday you had some problems during winter probably lexicon awards", k);
        queryProcessor.runQuery("AND I found out just yesterday", k);
        queryProcessor.runQuery("AND I found out just yesterday", k);
        queryProcessor.runQuery("OR May also get time", k);
        queryProcessor.runQuery("OR May also get time", k);
        queryProcessor.runQuery("AND I found out just yesterday you had some problems during winter probably lexicon awards", k);
        queryProcessor.runQuery("AND I found out just yesterday you had some problems during winter probably lexicon awards", k);
        queryProcessor.runQuery("OR May also get time", k);
    }

    @Test
    void runTestQueries() {
        int k = 100;
        ArrayList<List<String>> queries = new ArrayList<>();
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("./collection/queries.dev.small.tsv"))) {
            String line;
            while ((line = TSVReader.readLine()) != null) {
                String[] lines = line.split("\t");
                queries.add(List.of(lines));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("./collection/queries.results.txt"))) {
            SortedSet<DocumentScore> results;
            for (List<String> queryElem: queries) {
                StringBuilder output = new StringBuilder();
                String queryId = queryElem.get(0);
                String query = queryElem.get(1);
                results = queryProcessor.runQuery("OR " + query, k);
                int rank = 1;
                for (DocumentScore result: results) {
                    output.append(queryId).append(" Q0 ").append(result.docNo()).append(" ").append(rank++).append(" ").append(result.score()).append(" 01\n");
                }
                if (!output.isEmpty()) writer.append(String.valueOf(output));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
