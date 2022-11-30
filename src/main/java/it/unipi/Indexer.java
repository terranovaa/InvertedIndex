package it.unipi;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Indexer {

    // current doc id
    protected int currentId = 0;
    // useful for checking memory usage
    protected static final Runtime runtime = Runtime.getRuntime();
    // useful for giving different names to partial files
    protected int currentBlock = 0;
    // Value needs to be changed
    protected TreeMap<String, LexiconTerm> lexicon = new TreeMap<>();

    protected List<String> stopwords;

    public Indexer(String stopwordsPath) throws IOException{
        stopwords = Files.readAllLines(Paths.get(stopwordsPath));
    }

    public void indexCollection(String collectionPath) throws IOException {
        File file = new File(collectionPath);
        GzipCompressorInputStream gzInputStream = new GzipCompressorInputStream(new FileInputStream(file));
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzInputStream);
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // UTF8 or ASCII?
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;
            //TODO check if memory available
            while ((line = bufferedReader.readLine()) != null) {
                String doc_no = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);
                String[] tokens = tokenize(document);
                for (String token: tokens){
                    if(stopwords.contains(token)){
                        //if the token is a stopword don't consider it
                        continue;
                    }

                    //check if the token is already in the lexicon
                    LexiconTerm lexiconEntry;
                    if ((lexiconEntry = lexicon.get(token)) == null){
                        lexiconEntry = new LexiconTerm(token);
                        lexicon.put(token, lexiconEntry);
                    }
                    lexiconEntry.addToPostingList(currentId);
                }
                //move on to the next document
                currentId++;
                if(currentId > 2000){
                    break;
                }
            }
            for(LexiconTerm lexiconEntry: lexicon.values()){
                lexiconEntry.printInfo();
            }
        }
    }

    private String[] tokenize(String document){
        //TODO stemming
        document = document.toLowerCase();
        //remove punctuation and strange characters
        //TODO (to check whether it removes too many things or not)
        document = document.replaceAll("[^\\w\\s]", " ");
        //split in tokens
        return document.split("\\s");
    }
}
