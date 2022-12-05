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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Indexer {
    // current doc id
    protected int currentDocId = 0;
    //TODO to change (one term is too long, crashes after having processed around 1.5 millions documents)
    protected final int LEXICON_ENTRY_SIZE = 144;
    // TODO to change too, docno max size is 20 chars ok? 20*6 + 2*4
    protected final int DOCUMENT_ENTRY_SIZE = 128;
    //TODO how many terms do we have to cache?
    protected final int TERMS_TO_CACHE_DURING_MERGE = 10;
    protected final int DOCS_TO_CACHE_DURING_MERGE = 10;
    // useful for giving different names to partial files
    protected int currentBlock = 0;
    // Value needs to be changed
    protected TreeMap<String, LexiconTerm> lexicon = new TreeMap<>();
    protected HashMap<Integer, Document> documentTable = new HashMap<>();
    // TODO: use language detector to determine language of a word and give the support to many languages?
    protected SnowballStemmer stemmer;
    // TODO: same here?
    protected HashSet<String> stopWords = new HashSet<>();
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    public Indexer(boolean stopWordsOption, String stopWordsPath, boolean stemmingOption) throws IOException{
        if(stopWordsOption)
            stopWords.addAll(Files.readAllLines(Paths.get(stopWordsPath)));
        if(stemmingOption)
            stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
    }

    public void indexCollection(String collectionPath) throws IOException {
        File file = new File(collectionPath);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // UTF8 or ASCII?
            //other option that can be adapted Files.newBufferedReader(tarArchiveInputStream, StandardCharsets.UTF_8);
            // it throws MalformedInputException
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
                // DEBUG
                if (currentDocId % 100000 == 0)
                    System.out.println(currentDocId);

                String docNo = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);

                // check empty page
                if(document.length()==0)
                    continue;

                // add element to the document index
                documentTable.put(currentDocId, new Document(currentDocId, docNo, document.length()));

                String[] tokens = tokenize(document);
                for (String token: tokens){
                    //stop word removal & stemming
                    if(stopWords.size() != 0 && stopWords.contains(token)){
                        //if the token is a stop word don't consider it
                        continue;
                    }
                    if(stemmer != null)
                        token = (String) stemmer.stem(token);
                    //check if the token is already in the lexicon, if not create new entry
                    LexiconTerm lexiconEntry;
                    if ((lexiconEntry = lexicon.get(token)) == null){
                        lexiconEntry = new LexiconTerm(token);
                        lexicon.put(token, lexiconEntry);
                    }
                    lexiconEntry.addToPostingList(currentDocId);
                }

                // DEBUG
                if(currentDocId > 1000){
                    writeToDisk();
                    lexicon.clear();
                    break;
                }

                currentDocId++;
            }
        }
    }

    public void mergeBlocks(){
        try {
            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            //open all the needed files for each block
            ArrayList<InputStream> lexiconStreams = new ArrayList<>();
            ArrayList<InputStream> postingsDocIdsStreams = new ArrayList<>();
            ArrayList<InputStream> postingsFrequenciesStreams = new ArrayList<>();
            ArrayList<InputStream> documentIndexStreams = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconStreams.add(new BufferedInputStream(new FileInputStream("./resources/lexicon/lexicon_" + nextBlock + ".dat")));
                postingsDocIdsStreams.add(new BufferedInputStream(new FileInputStream("./resources/inverted_index/postings_doc_ids_" + nextBlock + ".dat")));
                postingsFrequenciesStreams.add(new BufferedInputStream(new FileInputStream("./resources/inverted_index/postings_frequencies_" + nextBlock + ".dat")));
                documentIndexStreams.add(new BufferedInputStream(new FileInputStream("./resources/documentIndex/documentIndex_" + nextBlock + ".dat")));
                nextBlock++;
            }

            //cache in memory of the first terms in the lexicon of each block
            byte[][] buffers = new byte[numberOfBlocks][LEXICON_ENTRY_SIZE * TERMS_TO_CACHE_DURING_MERGE];
            byte[][] nextLexiconEntry= new byte[numberOfBlocks][LEXICON_ENTRY_SIZE];
            LexiconTerm[] nextTerm = new LexiconTerm[numberOfBlocks];
            //used to keep track of current pointer in each buffer
            int[] pointers = new int[numberOfBlocks];

            String minTerm = null;

            for(int i=0; i < numberOfBlocks; i++){
                //read from file
                buffers[i] = lexiconStreams.get(i).readNBytes(LEXICON_ENTRY_SIZE * TERMS_TO_CACHE_DURING_MERGE);
                //get next entry
                nextLexiconEntry[i] = Arrays.copyOfRange(buffers[i], pointers[i], LEXICON_ENTRY_SIZE);
                nextTerm[i] = new LexiconTerm();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
                if(minTerm==null || nextTerm[i].getTerm().compareTo(minTerm) < 0){
                    minTerm = nextTerm[i].getTerm();
                }
            }

            //TODO while every lexicon has still terms remaining
            ArrayList<Integer> lexiconsToMerge = new ArrayList<>();

            //get the block indexes that have the current min term
            for(int i=0; i < numberOfBlocks; i++){
                if(nextTerm[i].getTerm().equals(minTerm)){
                    lexiconsToMerge.add(i);
                }
            }

            //create a new lexiconTerm object for the min term
            LexiconTerm referenceLexiconTerm = new LexiconTerm(nextTerm[lexiconsToMerge.get(0)].getTerm());

            //merge everything
            for (Integer blockIndex: lexiconsToMerge){
                LexiconTerm toMerge = nextTerm[blockIndex];
                //merge statistics
                referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + toMerge.getDocumentFrequency());
                referenceLexiconTerm.setCollectionFrequency(referenceLexiconTerm.getCollectionFrequency() + toMerge.getCollectionFrequency());
                //get posting list from disk
                byte[] postingDocIDs = postingsDocIdsStreams.get(blockIndex).readNBytes(toMerge.getDocIdsSize());
                byte[] postingFrequencies = postingsFrequenciesStreams.get(blockIndex).readNBytes(toMerge.getFrequenciesSize());
                ArrayList<Integer> docIDs = Utils.decode(postingDocIDs);
                ArrayList<Integer> frequencies = Utils.decode(postingFrequencies);
                //merge postings
                for (Integer docID: docIDs){
                    Integer frequency = frequencies.remove(0);
                    referenceLexiconTerm.addPosting(docID, frequency);
                }

                //TODO should work but didn't try
                //update pointers for every block
                pointers[blockIndex] += LEXICON_ENTRY_SIZE;
                if(pointers[blockIndex] == LEXICON_ENTRY_SIZE * TERMS_TO_CACHE_DURING_MERGE){
                    //if all the in-memory buffer is consumed, read again from file
                    buffers[blockIndex] = lexiconStreams.get(blockIndex).readNBytes(LEXICON_ENTRY_SIZE * TERMS_TO_CACHE_DURING_MERGE);
                    pointers[blockIndex] = 0;
                    nextLexiconEntry[blockIndex] = Arrays.copyOfRange(buffers[blockIndex], pointers[blockIndex], pointers[blockIndex] + LEXICON_ENTRY_SIZE);
                    nextTerm[blockIndex].deserialize(nextLexiconEntry[blockIndex]);
                } else{
                    //if not, read the next entry directly from the buffer
                    nextLexiconEntry[blockIndex] = Arrays.copyOfRange(buffers[blockIndex], pointers[blockIndex], pointers[blockIndex] + LEXICON_ENTRY_SIZE);
                    nextTerm[blockIndex].deserialize(nextLexiconEntry[blockIndex]);
                }
            }

            lexicon.put(referenceLexiconTerm.getTerm(), referenceLexiconTerm);

            // TODO: uncomment when finished
            //mergeDocumentIndexes();

            currentBlock = 100;
            writeToDisk();
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
        //TODO skip pointers if the sum of the doc frequencies is > 1024
    }

    private void mergeDocumentIndexes() throws IOException {
        int nextBlock=0;
        int numberOfBlocks=currentBlock+1;
        ArrayList<InputStream> documentInputIndexStreams = new ArrayList<>();
        String documentIndexFile = "./resources/documentIndex/documentIndex.dat";
        while(nextBlock < numberOfBlocks){
            documentInputIndexStreams.add(new BufferedInputStream(new FileInputStream("./resources/documentIndex/documentIndex_" + nextBlock + ".dat")));
            nextBlock++;
        }
        byte[] array;
        try (OutputStream documentOutputIndexStream = new BufferedOutputStream(new FileOutputStream(documentIndexFile))){
            for(int i = 0; i < numberOfBlocks; ++i) {
                array = documentInputIndexStreams.get(i).readNBytes(DOCUMENT_ENTRY_SIZE * DOCS_TO_CACHE_DURING_MERGE);
                while(array != null) {
                    documentOutputIndexStream.write(array);
                    array = documentInputIndexStreams.get(i).readNBytes(DOCUMENT_ENTRY_SIZE * DOCS_TO_CACHE_DURING_MERGE);
                }
            }
        }
    }

    private boolean memoryFull() {

        double MEMORY_THRESHOLD_PERCENTAGE = 0.75;

        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeap = memoryUsage.getUsed();
        long maxHeap = memoryUsage.getMax();
        // threshold percentage of the maxMemory allocated, under which we consider the memory as not free anymore
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

    private void writeToDisk(){

        String postingsDocIdsFile = "./resources/inverted_index/postings_doc_ids_" + currentBlock + ".dat";
        String postingsFrequenciesFile = "./resources/inverted_index/postings_frequencies_" + currentBlock + ".dat";
        String lexiconFile = "./resources/lexicon/lexicon_" + currentBlock + ".dat";
        String documentIndexFile = "./resources/documentIndex/documentIndex_" + currentBlock + ".dat";

        int docIDsFileOffset = 0;
        int frequenciesFileOffset = 0;

        long start = System.currentTimeMillis();

        try (OutputStream postingsDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
             OutputStream postingsFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
             OutputStream lexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));
             OutputStream documentIndexStream = new BufferedOutputStream(new FileOutputStream(documentIndexFile))
        ) {
            for (Map.Entry<String, LexiconTerm> entry : lexicon.entrySet()) {
                LexiconTerm lexiconTerm = entry.getValue();
                lexiconTerm.setDocIdsOffset(docIDsFileOffset);
                lexiconTerm.setFrequenciesOffset(frequenciesFileOffset);
                // docIDs
                List<Integer> docIDs = lexiconTerm.getPostingListDocIds();
                byte[] encodedDocIDs = Utils.encode(docIDs);
                docIDsFileOffset += encodedDocIDs.length;
                lexiconTerm.setDocIdsSize(encodedDocIDs.length);
                postingsDocIdsStream.write(encodedDocIDs);
                // frequencies
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                byte[] encodedFrequencies = Utils.encode(frequencies);
                frequenciesFileOffset += encodedFrequencies.length;
                lexiconTerm.setFrequenciesSize(encodedFrequencies.length);
                postingsFrequenciesStream.write(encodedFrequencies);
                // lexicon
                byte[] lexiconEntry = lexiconTerm.serialize();
                lexiconStream.write(lexiconEntry);
            }
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                byte[] documentIndexEntry = doc.getValue().serialize();
                documentIndexStream.write(documentIndexEntry);
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

    //encode lexiconTerm object as an array of bytes with fixed dimension
    // MOVED TO THE LexiconTerm CLASS
    private byte[] serializeLexiconEntry(LexiconTerm lexiconTerm) {
        byte[] lexiconEntry = new byte[LEXICON_ENTRY_SIZE];
        //variable number of bytes
        byte[] entryTerm = lexiconTerm.getTerm().getBytes(StandardCharsets.UTF_8);
        //fixed number of bytes, 4 for each integer
        byte[] entryDf = Utils.intToByteArray(lexiconTerm.getDocumentFrequency());
        byte[] entryCf = Utils.intToByteArray(lexiconTerm.getCollectionFrequency());
        byte[] entryDocIDOffset = Utils.intToByteArray(lexiconTerm.getDocIdsOffset());
        byte[] entryFrequenciesOffset = Utils.intToByteArray(lexiconTerm.getFrequenciesOffset());
        byte[] entryDocIDSize = Utils.intToByteArray(lexiconTerm.getDocIdsSize());
        byte[] entryFrequenciesSize = Utils.intToByteArray(lexiconTerm.getFrequenciesSize());
        //fill the first part of the buffer with the utf-8 representation of the term, leave the rest to 0
        System.arraycopy(entryTerm, 0, lexiconEntry, 0, entryTerm.length);
        //fill the last part of the buffer with statistics and offsets
        System.arraycopy(entryDf, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 24, 4);
        System.arraycopy(entryCf, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 20, 4);
        System.arraycopy(entryDocIDOffset, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 16, 4);
        System.arraycopy(entryFrequenciesOffset, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 12, 4);
        System.arraycopy(entryDocIDSize, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 8, 4);
        System.arraycopy(entryFrequenciesSize, 0, lexiconEntry, LEXICON_ENTRY_SIZE - 4, 4);
        return lexiconEntry;
    }

    //decode a disk-based array of bytes representing a lexicon entry in a LexiconTerm object
    // MOVED TO THE LexiconTerm CLASS
    private LexiconTerm deserializeLexiconEntry(byte[] buffer) {
        //to decode the term, detect the position of the first byte equal 0
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        String term = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        int documentFrequency = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 24);
        int collectionFrequency = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 20);
        int docIdOffset = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 16);
        int frequenciesOffset = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 12);
        int docIdSize = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 8);
        int frequenciesSize = Utils.byteArrayToInt(buffer, LEXICON_ENTRY_SIZE - 4);
        LexiconTerm lexiconTerm = new LexiconTerm(term);
        lexiconTerm.setDocumentFrequency(documentFrequency);
        lexiconTerm.setCollectionFrequency(collectionFrequency);
        lexiconTerm.setDocIdsOffset(docIdOffset);
        lexiconTerm.setFrequenciesOffset(frequenciesOffset);
        lexiconTerm.setDocIdsSize(docIdSize);
        lexiconTerm.setFrequenciesSize(frequenciesSize);
        return lexiconTerm;
    }
}
