package it.unipi;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;
import opennlp.tools.parser.Cons;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Indexer {

    // current doc id
    private int currentDocId = 0;
    // useful for giving different names to partial files
    private int currentBlock = 0;
    // Value needs to be changed
    private final TreeMap<String, LexiconTerm> lexicon = new TreeMap<>();
    private final HashMap<Integer, Document> documentTable = new HashMap<>();
    // TODO: use language detector to determine language of a word and give the support to many languages?
    private final SnowballStemmer stemmer;
    // TODO: same here?
    private final HashSet<String> stopWords = new HashSet<>();
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final String FILE_EXTENSION;

    public Indexer(String fileExtension) throws IOException{
        stopWords.addAll(Files.readAllLines(Paths.get(Constants.STOPWORDS_PATH)));
        stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
        FILE_EXTENSION = fileExtension;
    }

    public void indexCollection() throws IOException {

        File file = new File(Constants.COLLECTION_PATH);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // UTF8 or ASCII?
            // it throws MalformedInputException
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.US_ASCII));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // check if memory available
                if(memoryFull()) {
                    // TODO sometimes it makes two consecutive writes to disk, why? Maybe System.gc() is not working?
                    //  Possible solution: if(memoryFull() && !lexicon.size() < N) -J
                    writeToDisk();
                    currentBlock++;
                    lexicon.clear();
                    documentTable.clear();
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
                if(currentDocId > 1000000){
                    writeToDisk();
                    lexicon.clear();
                    documentTable.clear();
                    break;
                }

                currentDocId++;
            }
        }
    }

    public void mergeBlocks(){

        long start = System.currentTimeMillis();

        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.LEXICON_FILE_PATH + FILE_EXTENSION;

        try (OutputStream outputDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
             OutputStream outputFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
             OutputStream outputLexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile))){

            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            //open all the needed files for each block
            ArrayList<InputStream> lexiconStreams = new ArrayList<>();
            ArrayList<InputStream> postingsDocIdsStreams = new ArrayList<>();
            ArrayList<InputStream> postingsFrequenciesStreams = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_LEXICON_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsDocIdsStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsFrequenciesStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + nextBlock + FILE_EXTENSION)));
                nextBlock++;
            }

            //cache in memory of the first terms in the lexicon of each block
            byte[][] buffers = new byte[numberOfBlocks][Constants.LEXICON_ENTRY_SIZE * Constants.TERMS_TO_CACHE_DURING_MERGE];
            byte[][] nextLexiconEntry = new byte[numberOfBlocks][Constants.LEXICON_ENTRY_SIZE];
            LexiconTerm[] nextTerm = new LexiconTerm[numberOfBlocks];
            //used to keep track of current pointer in each buffer
            int[] pointers = new int[numberOfBlocks];
            //used to keep track of how many bytes were read the last time
            int[] bytesRead = new int[numberOfBlocks];
            ArrayList<Integer> activeBlocks = new ArrayList<>();

            String minTerm = null;

            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                bytesRead[i] = lexiconStreams.get(i).readNBytes(buffers[i], 0,Constants.LEXICON_ENTRY_SIZE * Constants.TERMS_TO_CACHE_DURING_MERGE);
                //get next entry
                nextLexiconEntry[i] = Arrays.copyOfRange(buffers[i], pointers[i], Constants.LEXICON_ENTRY_SIZE);
                nextTerm[i] = new LexiconTerm();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
                if(minTerm==null || nextTerm[i].getTerm().compareTo(minTerm) < 0){
                    minTerm = nextTerm[i].getTerm();
                }
            }

            while(activeBlocks.size() > 0){

                ArrayList<Integer> lexiconsToMerge = new ArrayList<>();

                minTerm = null;

                for(Integer blockIndex: activeBlocks){
                    if (minTerm == null || nextTerm[blockIndex].getTerm().compareTo(minTerm) < 0) {
                        minTerm = nextTerm[blockIndex].getTerm();
                    }
                }

                for(Integer blockIndex: activeBlocks){
                    if(nextTerm[blockIndex].getTerm().equals(minTerm)){
                        lexiconsToMerge.add(blockIndex);
                    }
                }

                //create a new lexiconTerm object for the min term
                LexiconTerm referenceLexiconTerm = new LexiconTerm(nextTerm[lexiconsToMerge.get(0)].getTerm());

                //merge everything
                for (Integer blockIndex: lexiconsToMerge){
                    LexiconTerm nextBlockToMerge = nextTerm[blockIndex];

                    //merge statistics
                    referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());
                    referenceLexiconTerm.setCollectionFrequency(referenceLexiconTerm.getCollectionFrequency() + nextBlockToMerge.getCollectionFrequency());

                    //get posting list from disk
                    byte[] postingDocIDs = postingsDocIdsStreams.get(blockIndex).readNBytes(nextBlockToMerge.getDocIdsSize());
                    byte[] postingFrequencies = postingsFrequenciesStreams.get(blockIndex).readNBytes(nextBlockToMerge.getFrequenciesSize());
                    ArrayList<Integer> docIDs = Utils.decode(postingDocIDs);
                    ArrayList<Integer> frequencies = Utils.decode(postingFrequencies);

                    //merge postings
                    for (Integer docID: docIDs){
                        Integer frequency = frequencies.remove(0);
                        referenceLexiconTerm.addPosting(docID, frequency);
                    }

                    //update pointers for every block
                    pointers[blockIndex] += Constants.LEXICON_ENTRY_SIZE;
                    if(pointers[blockIndex] >= bytesRead[blockIndex]){
                        if (bytesRead[blockIndex] < Constants.LEXICON_ENTRY_SIZE * Constants.TERMS_TO_CACHE_DURING_MERGE){
                            //if before we read less than those bytes, the relative block is finished
                            //blockIndex is not the index of the arraylist but an Integer object
                            activeBlocks.remove(blockIndex);
                        }
                        else{
                            //if all the in-memory buffer is consumed, refill it reading again from file
                            bytesRead[blockIndex] = lexiconStreams.get(blockIndex).readNBytes(buffers[blockIndex], 0, Constants.LEXICON_ENTRY_SIZE * Constants.TERMS_TO_CACHE_DURING_MERGE);
                            if(bytesRead[blockIndex] == 0){
                                activeBlocks.remove(blockIndex);
                            }
                            pointers[blockIndex] = 0;
                        }

                    }
                    if(activeBlocks.contains(blockIndex)){
                        //read the next entry from the buffer
                        nextLexiconEntry[blockIndex] = Arrays.copyOfRange(buffers[blockIndex], pointers[blockIndex], pointers[blockIndex] + Constants.LEXICON_ENTRY_SIZE);
                        nextTerm[blockIndex].deserialize(nextLexiconEntry[blockIndex]);
                    }
                }
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }

            mergePartialDocumentTables();
            long end = System.currentTimeMillis();
            System.out.println("Merged in " + (end - start) + " ms");

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
        //TODO skip pointers if the sum of the doc frequencies is > 1024
    }

    private void mergePartialDocumentTables() throws IOException {

        int nextBlock=0;
        int numberOfBlocks=currentBlock+1;
        String[] documentTableInputFiles = new String[currentBlock+1];
        while(nextBlock < numberOfBlocks){
            documentTableInputFiles[nextBlock] = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + nextBlock + FILE_EXTENSION;
            nextBlock++;
        }
        String documentTableOutputFile = Constants.DOCUMENT_TABLE_FILE_PATH + FILE_EXTENSION;

        FileOutputStream fos = new FileOutputStream(documentTableOutputFile);
        WritableByteChannel targetChannel = fos.getChannel();

        for (String documentTableInputFile : documentTableInputFiles) {
            //Get channel for input files
            FileInputStream fis = new FileInputStream(documentTableInputFile);
            FileChannel inputChannel = fis.getChannel();

            //Transfer data from input channel to output channel
            inputChannel.transferTo(0, inputChannel.size(), targetChannel);

            //close the input channel
            inputChannel.close();
            fis.close();
        }
        /*
        ArrayList<InputStream> documentInputIndexStreams = new ArrayList<>();
        String documentIndexFile = "./resources/document_index/document_index.dat";
        while(nextBlock < numberOfBlocks){
            documentInputIndexStreams.add(new BufferedInputStream(new FileInputStream("./resources/document_index/document_index_" + nextBlock + ".dat")));
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

         */
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

        String postingsDocIdsFile = Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + currentBlock + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + currentBlock + FILE_EXTENSION;
        String lexiconFile = Constants.PARTIAL_LEXICON_FILE_PATH + currentBlock + FILE_EXTENSION;
        String documentTableFile = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + currentBlock + FILE_EXTENSION;

        int docIDsFileOffset = 0;
        int frequenciesFileOffset = 0;

        long start = System.currentTimeMillis();

        try (OutputStream postingsDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
             OutputStream postingsFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
             OutputStream lexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));
             OutputStream documentTableStream = new BufferedOutputStream(new FileOutputStream(documentTableFile))
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
                byte[] documentTableEntry = doc.getValue().serialize();
                documentTableStream.write(documentTableEntry);
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

}
