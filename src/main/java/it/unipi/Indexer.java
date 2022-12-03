package it.unipi;

import it.unipi.utils.Utils;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
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

    protected final String postingsDocIdsFile = "./resources/inverted_index/postings_doc_ids_" + currentBlock + ".dat";
    protected final String postingsFrequenciesFile = "./resources/inverted_index/postings_frequencies_" + currentBlock + ".dat";
    protected final String lexiconFile = "./resources/lexicon/lexicon_" + currentBlock + ".dat";


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
                    writeToDisk();
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

    public void writeToDisk(){

        int docIDsFileOffset = 0;
        int frequenciesFileOffset = 0;

        long start = System.currentTimeMillis();

        try (
                OutputStream postingsDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
                OutputStream postingsFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
                OutputStream lexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile))
                ) {

            for (Map.Entry<String, LexiconTerm> entry : lexicon.entrySet()) {
                LexiconTerm lexiconTerm = entry.getValue();
                lexiconTerm.setDocIDsOffset(docIDsFileOffset);
                lexiconTerm.setFrequenciesOffset(frequenciesFileOffset);
                // docIDs
                List<Integer> docIDs = lexiconTerm.getPostingList().stream().map(Posting::getDocID).toList();
                byte[] encodedDocIDs = Utils.encode(docIDs);
                docIDsFileOffset += encodedDocIDs.length;
                postingsDocIdsStream.write(encodedDocIDs);
                // frequencies
                List<Integer> frequencies = lexiconTerm.getPostingList().stream().map(Posting::getFrequency).toList();
                byte[] encodedFrequencies = Utils.encode(frequencies);
                frequenciesFileOffset += encodedFrequencies.length;
                postingsFrequenciesStream.write(encodedFrequencies);
                // lexicon
                byte[] lexiconEntry = serializeLexiconEntry(lexiconTerm);
                lexiconStream.write(lexiconEntry);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long end = System.currentTimeMillis();

        System.out.println("Copied in " + (end - start) + " ms");
    }

    // TODO
    public void mergeBlocks(){
        // TODO check if writeToDisk actually works
        try(
            OutputStream postingsDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
            OutputStream postingsFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
            InputStream lexiconStream = new BufferedInputStream(new FileInputStream(lexiconFile))
            )
        {
            byte[] buffer = lexiconStream.readNBytes(136);
            LexiconTerm lexiconTerm = deserializeLexiconEntry(buffer);
            lexiconTerm.printInfo();
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
        // exploit currentBlock to determine the number and names of files we need to merge
        // Use the blocked sort-based indexing merging algorithm
        // skip pointers if the sum of the doc frequencies is > 1024
    }


    //encode lexiconTerm object as an array of bytes with fixed dimension
    private byte[] serializeLexiconEntry(LexiconTerm lexiconTerm) {
        // TODO: how many characters per word? 20?
        byte[] lexiconEntry = new byte[136];
        //variable number of bytes
        byte[] entryTerm = lexiconTerm.getTerm().getBytes(StandardCharsets.UTF_8);
        //fixed number of bytes, 4 for each integer
        byte[] entryDf = Utils.intToByteArray(lexiconTerm.getDocumentFrequency());
        byte[] entryCf = Utils.intToByteArray(lexiconTerm.getCollectionFrequency());
        byte[] entryDocIDOffset = Utils.intToByteArray(lexiconTerm.getDocIDsOffset());
        byte[] entryFrequenciesOffset = Utils.intToByteArray(lexiconTerm.getFrequenciesOffset());
        //fill the first part of the buffer with the utf-8 representation of the term, leave the rest to 0
        System.arraycopy(entryTerm, 0, lexiconEntry, 0, entryTerm.length);
        //fill the last part of the buffer with statistics and offsets
        System.arraycopy(entryDf, 0, lexiconEntry, 120, 4);
        System.arraycopy(entryCf, 0, lexiconEntry, 124, 4);
        System.arraycopy(entryDocIDOffset, 0, lexiconEntry, 128, 4);
        System.arraycopy(entryFrequenciesOffset, 0, lexiconEntry, 132, 4);
        return lexiconEntry;
    }

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTerm object
    private LexiconTerm deserializeLexiconEntry(byte[] buffer) {
        //to decode the term, detect the position of the first byte equal 0
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        String term = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        int documentFrequency = Utils.byteArrayToInt(buffer, 120);
        int collectionFrequency = Utils.byteArrayToInt(buffer, 124);
        int docIdOffset = Utils.byteArrayToInt(buffer, 128);
        int frequenciesOffset = Utils.byteArrayToInt(buffer, 132);
        LexiconTerm lexiconTerm = new LexiconTerm(term);
        lexiconTerm.setDocumentFrequency(documentFrequency);
        lexiconTerm.setCollectionFrequency(collectionFrequency);
        lexiconTerm.setDocIDsOffset(docIdOffset);
        lexiconTerm.setFrequenciesOffset(frequenciesOffset);
        return lexiconTerm;
    }
}
