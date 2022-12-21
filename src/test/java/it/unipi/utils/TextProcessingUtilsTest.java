package it.unipi.utils;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class TextProcessingUtilsTest {

    @Test
    void stemmingComparisonTest() throws IOException {

        File file = new File(Constants.COLLECTION_PATH);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // it uses MalformedInputException internally and replace the malformed character as default operation
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // check if the occupied memory has reached the threshold

                // doc_no and document are split by \t
                String docNo = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);

                // if the document is empty we skip it
                if (document.length() == 0) continue;

                // We remove punctuation and split the document into tokens
                String[] tokens = TextProcessingUtils.tokenize(document);

                for (String token : tokens) {
                    //if the token is a stop word don't consider it
                    if (TextProcessingUtils.isAStopWord(token))
                        continue;
                    // if the token is longer than 20 chars we truncate it
                    token = TextProcessingUtils.truncateToken(token);
                    String stemmerToken = TextProcessingUtils.stemToken(token);
                    //String porterStemmerToken = TextProcessingUtils.porterStemmer(token);
                    //assert stemmerToken.equals(porterStemmerToken);
                }
            }
        } else {
            throw new RuntimeException("There was a problem reading the .tar.gz file");
        }
    }

    @Test
    void stemmingTimeComparisonTest() throws IOException {

        File file = new File(Constants.COLLECTION_PATH);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // it uses MalformedInputException internally and replace the malformed character as default operation
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;
            long start;
            long timeElapsedStemmer = 0;
            long timeElapsedPorterStemmer = 0;
            long timeElapsedSnowballStemmer = 0;
            int currentDoc = 0;
            while ((line = bufferedReader.readLine()) != null) {
                // check if the occupied memory has reached the threshold

                // doc_no and document are split by \t
                String document = line.substring(line.indexOf("\t") + 1);

                // if the document is empty we skip it
                if (document.length() == 0) continue;

                // We remove punctuation and split the document into tokens
                String[] tokens = TextProcessingUtils.tokenize(document);

                for (String token : tokens) {
                    //if the token is a stop word don't consider it
                    if (TextProcessingUtils.isAStopWord(token))
                        continue;
                    // if the token is longer than 20 chars we truncate it
                    token = TextProcessingUtils.truncateToken(token);

                    start = System.currentTimeMillis();
                    TextProcessingUtils.stemToken(token);
                    timeElapsedStemmer += System.currentTimeMillis() - start;

                    /*
                    start = System.currentTimeMillis();
                    TextProcessingUtils.porterStemmer(token);
                    timeElapsedPorterStemmer += System.currentTimeMillis() - start;

                    start = System.currentTimeMillis();
                    TextProcessingUtils.stemToken(token);
                    timeElapsedSnowballStemmer += System.currentTimeMillis() - start;

                     */
                }

                currentDoc++;

                if (currentDoc % 100_000 == 0) System.out.println(currentDoc);
            }
            System.out.println(timeElapsedStemmer);
            System.out.println(timeElapsedPorterStemmer);
            System.out.println(timeElapsedSnowballStemmer);
        } else {
            throw new RuntimeException("There was a problem reading the .tar.gz file");
        }
    }
}
