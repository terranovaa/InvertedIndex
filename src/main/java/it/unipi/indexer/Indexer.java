package it.unipi.indexer;

import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.LexiconTermIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.TextProcessingUtils;
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
import java.util.*;
import java.util.function.Supplier;

// class is parametrized due to the fact that indexing can be either Binary or Textual (ASCII)
abstract public class Indexer <T extends LexiconTermIndexing> {
    // current doc id
    protected int currentDocId = 0;
    // useful for giving different names to partial files
    protected int currentBlock = 0;
    // partial lexicon, using a TreeMap in order to have lexicographical order (inserting operation is O(log(N)))
    protected final TreeMap<String, T> lexicon = new TreeMap<>();
    // used to call the right constructor based on the type of T
    private final Supplier<? extends T> lexiconTermConstructor;
    // doc table, ysing a LinkedHashMap because we need to maintain the insertion order
    protected final LinkedHashMap<Integer, Document> documentTable = new LinkedHashMap<>();
    // collection statistics
    protected final CollectionStatistics collectionStatistics = new CollectionStatistics();
    // used to check for memory occupancy during indexing
    protected final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    // can be .txt or .dat
    protected final String FILE_EXTENSION;
    // number of terms
    private int numTerms = 0;

    public Indexer(Supplier<? extends T> lexiconTermConstructor, String fileExtension) {
        this.lexiconTermConstructor = lexiconTermConstructor;
        FILE_EXTENSION = fileExtension.toLowerCase();
        System.out.println("Using "+ FILE_EXTENSION + " as file extension..");
    }

    public void indexCollection() throws IOException {
        File file = new File(Constants.COLLECTION_PATH);
        // reading the compressed tar.gz file
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        if (tarArchiveEntry != null) {
            // it uses MalformedInputException internally and replace the malformed character as default operation
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.UTF_8));
            String line;
            // reading one line at a time (one line corresponds to one document)
            while ((line = bufferedReader.readLine()) != null) {
                // checking if the used memory has reached the threshold
                checkMemory();

                // doc_no and document are separated by \t
                String docNo = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);

                // if the document is empty we move on
                if(document.length() == 0) continue;

                int docLen = 0;

                // Removing punctuation and splitting the document into tokens
                String[] tokens = TextProcessingUtils.tokenize(document);

                for (String token: tokens) {
                    // if the token is a stop word move on
                    if (TextProcessingUtils.isAStopWord(token))
                        continue;
                    docLen++;
                    // if the token is longer than 20 chars we truncate it
                    token = TextProcessingUtils.truncateToken(token);
                    // applying the snowball stemmer
                    token = TextProcessingUtils.stemToken(token);
                    // if the token is not already in the lexicon we create a new entry
                    T lexiconEntry;
                    if ((lexiconEntry = lexicon.get(token)) == null) { // O(1)
                        lexiconEntry = lexiconTermConstructor.get(); // calls the constructor based on T
                        lexiconEntry.setTerm(token);
                        lexicon.put(token, lexiconEntry);
                        // updating collection statistics
                        numTerms++;
                    }
                    // if the docId is already in the posting of the term we increase its frequency, otherwise we add it to the list with frequency 1
                    lexiconEntry.addToPostingList(currentDocId);
                }

                // used for checking progress
                if (currentDocId % 100000 == 0) {
                    System.out.println("Analyzing document n. " + currentDocId);
                }

                // if the document contains only stopwords, we move on
                if (docLen == 0) continue;

                // saving the document in the doc
                documentTable.put(currentDocId, new Document(currentDocId, docNo, docLen));

                currentDocId++;
            }

            // final statistics
            collectionStatistics.setNumDocs(currentDocId);
            collectionStatistics.setAvgDocLen((double) numTerms / currentDocId);

            writeBlockToDisk();
            lexicon.clear();
            documentTable.clear();
        } else {
            throw new RuntimeException("There was a problem reading the .tar.gz file");
        }
    }

    // function used for checking used heap
    protected void checkMemory(){
        if (memoryAboveThreshold(Constants.MEMORY_FULL_THRESHOLD_PERCENTAGE)) {
            writeBlockToDisk();
            currentBlock++;
            lexicon.clear();
            documentTable.clear();
            System.out.println("Start asking the gc to come in action until we reach " + Constants.MEMORY_ENOUGH_THRESHOLD_PERCENTAGE * 100 + "%..");
            // use a while loop since we don't know when the garbage collector will come in action
            while (memoryAboveThreshold(Constants.MEMORY_ENOUGH_THRESHOLD_PERCENTAGE))
                System.gc();
            System.out.println("Enough memory now..");
        }
    }

    protected boolean memoryAboveThreshold(double MEMORY_THRESHOLD_PERCENTAGE) {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeap = memoryUsage.getUsed();
        long maxHeap = memoryUsage.getMax();
        // threshold percentage of the maxMemory allocated, under which we consider the memory as not free anymore
        double heapThreshold = MEMORY_THRESHOLD_PERCENTAGE * maxHeap;
        return usedHeap >= heapThreshold;
    }

    // these functions are abstract because their implementation depends on the type of indexing
    abstract void writeBlockToDisk();

    abstract public void mergeBlocks();

    // same function for both binary and textual indexing, it just concatenates the partial files
    protected void mergePartialDocumentTables() throws IOException {

        int nextBlock=0;
        int numberOfBlocks=currentBlock + 1;

        String[] documentTableInputFiles = new String[currentBlock+1];

        // opening the partial files
        while(nextBlock < numberOfBlocks){
            documentTableInputFiles[nextBlock] = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + nextBlock + FILE_EXTENSION;
            nextBlock++;
        }
        String documentTableOutputFile = Constants.DOCUMENT_TABLE_FILE_PATH + FILE_EXTENSION;

        FileOutputStream fos = new FileOutputStream(documentTableOutputFile);
        WritableByteChannel targetChannel = fos.getChannel();

        for (String documentTableInputFile : documentTableInputFiles) {
            // getting channel for input files
            FileInputStream fis = new FileInputStream(documentTableInputFile);
            FileChannel inputChannel = fis.getChannel();

            // transferring data from input channel to output channel
            inputChannel.transferTo(0, inputChannel.size(), targetChannel);

            // closing the input channel
            inputChannel.close();
            fis.close();
        }
    }

    // this function gets the indexes of the blocks containing the minimum term in lexicographical order
    protected ArrayList<Integer> getBlocksToMerge(List<Integer> activeBlocks, T[] nextTerm) {

        ArrayList<Integer> blocksToMerge = new ArrayList<>();

        String minTerm = null;

        //searching for the minimum term among the blocks that aren't finished yet
        for(Integer blockIndex: activeBlocks){
            if (minTerm == null || nextTerm[blockIndex].getTerm().compareTo(minTerm) < 0) {
                minTerm = nextTerm[blockIndex].getTerm();
            }
        }

        //getting the blocks that contain the current minimum term
        for(Integer blockIndex: activeBlocks){
            if(nextTerm[blockIndex].getTerm().equals(minTerm)){
                blocksToMerge.add(blockIndex);
            }
        }

        return blocksToMerge;
    }
}
