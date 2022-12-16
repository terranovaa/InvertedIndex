package it.unipi.indexer;

import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.LexiconTermIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Supplier;

abstract public class Indexer <T extends LexiconTermIndexing> {

    // current doc id
    protected int currentDocId = 0;
    // useful for giving different names to partial files
    protected int currentBlock = 0;
    // Value needs to be changed
    protected final TreeMap<String, T> lexicon = new TreeMap<>();
    private final Supplier<? extends T> lexiconTermConstructor;
    protected final HashMap<Integer, Document> documentTable = new HashMap<>();
    protected final CollectionStatistics collectionStatistics = new CollectionStatistics();
    protected final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    protected final String FILE_EXTENSION;

    public Indexer(Supplier<? extends T> lexiconTermConstructor, String fileExtension) {
        this.lexiconTermConstructor = lexiconTermConstructor;
        FILE_EXTENSION = fileExtension;
        System.out.println("Using "+ FILE_EXTENSION + " as file extension..");
    }

    abstract public void merge();

    abstract void writeToDisk();

    public void indexCollection() throws IOException {
        File file = new File(Constants.COLLECTION_PATH);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) { //TODO: UTF8 or ASCII?
            // it uses MalformedInputException internally and replace the malformed character as default operation
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;

            while ((line = bufferedReader.readLine()) != null) {

                // check if the occupied memory has reached the threshold
                if (checkMemory(Constants.MEMORY_FULL_THRESHOLD_PERCENTAGE)) {

                    writeToDisk();
                    currentBlock++;
                    lexicon.clear();
                    documentTable.clear();
                    System.out.println("Start asking the gc to come in action until we reach " + Constants.MEMORY_ENOUGH_THRESHOLD_PERCENTAGE * 100 + "%..");
                    // use a while loop since we don't know when the garbage collector will come in action
                    while (checkMemory(Constants.MEMORY_ENOUGH_THRESHOLD_PERCENTAGE))
                        System.gc();
                    System.out.println("Enough memory now..");
                }

                // doc_no and document are split by \t
                String docNo = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);

                // if the document is empty we skip it
                if(document.length() == 0) continue;

                // we add the document to the document table
                //TODO here length includes also stopwords, is it correct?
                documentTable.put(currentDocId, new Document(currentDocId, docNo, document.length()));

                // We remove punctuation and split the document into tokens
                String[] tokens = Utils.tokenize(document);

                for (String token: tokens){

                    //if the token is a stop word don't consider it
                    if(Utils.isAStopWord(token)) continue;

                    // if the token is longer than 20 chars we truncate it
                    token = Utils.truncateToken(token);
                    // we apply the snowball stemmer
                    token = Utils.stemToken(token);

                    // updating collection statistics
                    collectionStatistics.incrementNumTotalTerms();

                    //check if the token is already in the lexicon, if not create new entry
                    T lexiconEntry;
                    if ((lexiconEntry = lexicon.get(token)) == null){
                        lexiconEntry = lexiconTermConstructor.get();
                        lexiconEntry.setTerm(token);
                        lexicon.put(token, lexiconEntry);
                    }
                    lexiconEntry.addToPostingList(currentDocId);
                }

                // DEBUG
                if (currentDocId % 100000 == 0) {
                    System.out.println(currentDocId);
                }

                currentDocId++;
            }

            writeToDisk();
            lexicon.clear();
            documentTable.clear();
        } else {
            throw new RuntimeException("There was a problem reading the .tar.gz file");
        }
    }

    protected boolean checkMemory(double MEMORY_THRESHOLD_PERCENTAGE) {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeap = memoryUsage.getUsed();
        long maxHeap = memoryUsage.getMax();
        // threshold percentage of the maxMemory allocated, under which we consider the memory as not free anymore
        double heapThreshold = MEMORY_THRESHOLD_PERCENTAGE * maxHeap;
        return usedHeap >= heapThreshold;
    }

    // same function for both binary and textual indexing, it just concatenates the partial files
    protected void mergePartialDocumentTables() throws IOException {
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
    }

    protected ArrayList<Integer> getLexiconsToMerge(List<Integer> activeBlocks, T[] nextTerm) {

        ArrayList<Integer> lexiconsToMerge = new ArrayList<>();

        String minTerm = null;
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

        return lexiconsToMerge;
    }
}
