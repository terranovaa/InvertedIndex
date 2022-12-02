package it.unipi;

import opennlp.tools.stemmer.snowball.SnowballStemmer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Indexer {
    // current doc id
    protected int currentId = 0;
    // threshold percentage of the maxMemory allocated, under which we consider the memory as not free anymore
    private final double MEMORY_THRESHOLD_PERCENTAGE = 0.75;
    // useful for giving different names to partial files
    protected int currentBlock = 0;
    // Value needs to be changed
    protected TreeMap<String, LexiconTerm> lexicon = new TreeMap<>();
    // TODO: use language detector to determine language of a word and give the support to many languages?
    protected SnowballStemmer stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
    // TODO: same here?
    protected HashSet<String> stopwords = new HashSet<>();

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    public Indexer(String stopwordsPath) throws IOException{
        stopwords.addAll(Files.readAllLines(Paths.get(stopwordsPath)));
    }

    public void indexCollection(String collectionPath) throws IOException {
        File file = new File(collectionPath);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // UTF8 or ASCII?
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // check if memory available
                if(memoryFull()) {
                    //writeToDisk("file" + currentBlock); // TODO write postings and lexicon to disk
                    currentBlock++;
                    lexicon.clear();

                    // use a while loop since we don't know when the garbage collector will come in action
                    while(memoryFull()) {
                        System.gc();
                    }
                }
                if (currentId % 100000 == 0)
                    System.out.println(currentId);
                String doc_no = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);
                String[] tokens = tokenize(document);
                for (String token: tokens){
                    //stopword removal & stemming
                    if(stopwords.contains(token)){
                        //if the token is a stopword don't consider it
                        continue;
                    }
                    token = (String) stemmer.stem(token);
                    //check if the token is already in the lexicon, if not create new entry
                    LexiconTerm lexiconEntry;
                    if ((lexiconEntry = lexicon.get(token)) == null){
                        lexiconEntry = new LexiconTerm(token);
                        lexicon.put(token, lexiconEntry);
                    }
                    lexiconEntry.addToPostingList(currentId);
                }
                //move on to the next document
                currentId++;
                if(currentId > 5000000){
                    break;
                }
            }
        }
        mergeBlocks();
    }

    private boolean memoryFull() {

        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeap = memoryUsage.getUsed();
        long maxHeap = memoryUsage.getMax();
        double heapThreshold = MEMORY_THRESHOLD_PERCENTAGE * maxHeap;
        return usedHeap >= heapThreshold;
    }

    private String[] tokenize(String document){
        document = document.toLowerCase();
        //remove punctuation and strange characters
        document = document.replaceAll("[^\\w\\s]", " ");
        // handle lower case in order to be the same thing
        //split in tokens
        // TODO: or \\s+
        return document.split("\\s");
    }

    public void writeToDisk(String filename){
        // TODO: Franco this function fucking sucks what the hell
        ObjectOutputStream objStream;
        ByteArrayOutputStream byteStream;
        try {
            byteStream = new ByteArrayOutputStream();
            objStream = new ObjectOutputStream(byteStream);;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for(Map.Entry<String,LexiconTerm> entry : lexicon.entrySet()) {
            LexiconTerm value = entry.getValue();
            try {
                objStream.writeObject(value);
                objStream.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        byte[] bytes = byteStream.toByteArray();
        try (FileOutputStream fileStream = new FileOutputStream("./resources/"+filename)) {
            fileStream.write(bytes);
            // no fos.closeclose since the instance is inside the try and this will automatically close the FileOutputStream
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO
    public void mergeBlocks(){
        // exploit currentBlock to determine the number and names of files we need to merge
        // Use the blocked sort-based indexing merging algorithm
    }

    // TODO: Consider this possible solution to force the garbage collector to came into action instead of just giving the hint
    public static void gc() {
        Object obj = new Object();
        WeakReference<Object> ref = new WeakReference<>(obj);
        obj = null;
        while(ref.get() != null) {
            System.gc();
        }
    }
}
