package it.unipi;

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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

import org.tartarus.snowball.ext.englishStemmer;

import static it.unipi.utils.Utils.validDocument;
import static it.unipi.utils.Utils.validToken;

public class Indexer {

    //TODO: Collection statistics
    // current doc id
    private int currentDocId = 0;
    // useful for giving different names to partial files
    private int currentBlock = 0;
    // Value needs to be changed
    private final TreeMap<String, LexiconTerm> lexicon = new TreeMap<>();
    private final HashMap<Integer, Document> documentTable = new HashMap<>();
    // TODO choose between different Stemmers
    // SnowballStemmer slowest
    // PorterStemmer fastest but older version of stemming
    // englishStemmer same as SnowballStemmer but faster
    //private final SnowballStemmer stemmer;
    //private final PorterStemmer porterStemmer;
    private final englishStemmer englishStemmer = new englishStemmer();
    private final HashSet<String> stopWords = new HashSet<>();
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final String FILE_EXTENSION;
    private final double MEMORY_FULL_THRESHOLD_PERCENTAGE = 0.75;
    private final double MEMORY_ENOUGH_THRESHOLD_PERCENTAGE = 0.25;

    public Indexer(String fileExtension) throws IOException{
        stopWords.addAll(Files.readAllLines(Paths.get(Constants.STOPWORDS_PATH)));
        //stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
        //porterStemmer = new PorterStemmer();
        FILE_EXTENSION = fileExtension;
    }

    public void indexCollection() throws IOException {
        File file = new File(Constants.COLLECTION_PATH);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(file)));
        TarArchiveEntry tarArchiveEntry = tarArchiveInputStream.getNextTarEntry();
        BufferedReader bufferedReader;
        //long start = System.currentTimeMillis();
        if (tarArchiveEntry != null) { //TODO: UTF8 or ASCII?
            // it uses MalformedInputException internally and replace the malformed character as default operation
            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream, StandardCharsets.US_ASCII));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // check if memory available
                // TODO: check if necessary
                if (currentDocId % 100000 == 0) {
                    System.out.println(currentDocId);
                    if (checkMemory(MEMORY_FULL_THRESHOLD_PERCENTAGE)) {
                        writeToDisk();
                        currentBlock++;
                        lexicon.clear();
                        documentTable.clear();
                        System.out.println("Start asking the gc to come in action until we reach " + MEMORY_ENOUGH_THRESHOLD_PERCENTAGE * 100 + "%..");
                        while (checkMemory(MEMORY_ENOUGH_THRESHOLD_PERCENTAGE))  // use a while loop since we don't know when the garbage collector will come in action
                            System.gc();
                        System.out.println("Enough memory now..");
                    }
                }

                String docNo = line.substring(0, line.indexOf("\t"));
                String document = line.substring(line.indexOf("\t") + 1);

                if(!validDocument(document))
                    continue;

                // add element to the document index
                documentTable.put(currentDocId, new Document(currentDocId, docNo, document.length()));

                String[] tokens = tokenize(document);
                for (String token: tokens){
                    if(!validToken(token, stopWords))
                        continue;

                    //String token = porterStemmer.stem(token);
                    //String token2 = (String) stemmer.stem(token);
                    englishStemmer.setCurrent(token);
                    if (englishStemmer.stem()) {
                        token = englishStemmer.getCurrent();
                    }

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
                    System.out.println(documentTable.size());
                    documentTable.clear();
                    break;
                }


                currentDocId++;
            }
            writeToDisk();
            lexicon.clear();
            documentTable.clear();
        }
    }

    private boolean checkMemory(double MEMORY_THRESHOLD_PERCENTAGE) {
        MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
        long usedHeap = memoryUsage.getUsed();
        long maxHeap = memoryUsage.getMax();
        // threshold percentage of the maxMemory allocated, under which we consider the memory as not free anymore
        double heapThreshold = MEMORY_THRESHOLD_PERCENTAGE * maxHeap;
        return usedHeap >= heapThreshold;
    }

    private String[] tokenize(String document){
        // normalization
        document = document.toLowerCase();
        // TODO: more complex tokenization? Like BPE?
        //remove punctuation and strange characters
        document = document.replaceAll("[^a-z0-9\\s]", " ");
        //split in tokens
        return document.split(" ");
    }

    private void writeToDisk(){
        if(Objects.equals(FILE_EXTENSION, Constants.DAT_FORMAT))
            writeToDiskBinary();
        else if(Objects.equals(FILE_EXTENSION, Constants.TXT_FORMAT))
            writeToDiskTextual();
    }

    private void writeToDiskBinary(){
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
                byte[] lexiconEntry = lexiconTerm.serializeBinary();
                lexiconStream.write(lexiconEntry);
            }
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                byte[] documentTableEntry = doc.getValue().serializeBinary();
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

    private void writeToDiskTextual(){
        String postingsDocIdsFile = Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + currentBlock + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + currentBlock + FILE_EXTENSION;
        String lexiconFile = Constants.PARTIAL_LEXICON_FILE_PATH + currentBlock + FILE_EXTENSION;
        String documentTableFile = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + currentBlock + FILE_EXTENSION;

        long start = System.currentTimeMillis();
        try (BufferedWriter postingsDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter postingsFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter lexiconStream = new BufferedWriter(new FileWriter(lexiconFile));
             BufferedWriter documentTableStream = new BufferedWriter(new FileWriter(documentTableFile))
        ) {
            for (Map.Entry<String, LexiconTerm> entry : lexicon.entrySet()) {
                LexiconTerm lexiconTerm = entry.getValue();
                //docIDs
                List<Integer> docIDs = lexiconTerm.getPostingListDocIds();
                for(int i = 0; i < docIDs.size(); ++i)
                    if(i != docIDs.size()-1)
                        postingsDocIdsStream.write(docIDs.get(i).toString()+",");
                    else postingsDocIdsStream.write(docIDs.get(i).toString()+"\n");
                // frequencies
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                for(int i = 0; i < frequencies.size(); ++i)
                    if(i != docIDs.size()-1)
                        postingsFrequenciesStream.write(frequencies.get(i).toString()+",");
                    else postingsFrequenciesStream.write(frequencies.get(i).toString()+"\n");
                //lexicon terms
                String[] lexiconEntry = lexiconTerm.serializeTextual();
                for(int i = 0; i < lexiconEntry.length; ++i)
                    if(i != lexiconEntry.length-1)
                        lexiconStream.write(lexiconEntry[i]+",");
                    else lexiconStream.write(lexiconEntry[i]+"\n"); // since we don't have the offset information here, we use \n as delimiter
            }
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                String[] documentTableEntry = doc.getValue().serializeTextual();
                for(int i = 0; i < documentTableEntry.length; ++i)
                    if(i != documentTableEntry.length-1)
                        documentTableStream.write(documentTableEntry[i]+",");
                    else documentTableStream.write(documentTableEntry[i]+"\n"); // since we don't have the offset information here, we use \n as delimiter
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("Partial files written in " + (end - start) + " ms");
    }

    public void mergeBlocks(){
        if(Objects.equals(FILE_EXTENSION, Constants.DAT_FORMAT))
            mergeBlocksBinary();
        else if(Objects.equals(FILE_EXTENSION, Constants.TXT_FORMAT))
            mergeBlocksTextual();
    }

    public void mergeBlocksBinary(){

        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.LEXICON_FILE_PATH + FILE_EXTENSION;

        try {
            FileOutputStream outputDocIdsStream = new FileOutputStream(postingsDocIdsFile);
            FileOutputStream outputFrequenciesStream = new FileOutputStream(postingsFrequenciesFile);
            OutputStream outputLexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));
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

            byte[][] nextLexiconEntry = new byte[numberOfBlocks][Constants.LEXICON_ENTRY_SIZE];
            LexiconTerm[] nextTerm = new LexiconTerm[numberOfBlocks];
            //used to keep track of how many bytes were read the last time
            int[] bytesRead = new int[numberOfBlocks];
            ArrayList<Integer> activeBlocks = new ArrayList<>();

            String minTerm = null;

            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                bytesRead[i] = lexiconStreams.get(i).readNBytes(nextLexiconEntry[i], 0,Constants.LEXICON_ENTRY_SIZE);
                nextTerm[i] = new LexiconTerm();
                nextTerm[i].deserializeBinary(nextLexiconEntry[i]);
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
                    referenceLexiconTerm.mergeEncodedPostings(postingDocIDs, postingFrequencies);

                    if(bytesRead[blockIndex] < Constants.LEXICON_ENTRY_SIZE){
                        //if before we read less than those bytes, the relative block is finished
                        //blockIndex is not the index of the arraylist but an Integer object
                        activeBlocks.remove(blockIndex);
                    }
                    if(activeBlocks.contains(blockIndex)){
                        //read the next entry from the block
                        bytesRead[blockIndex] = lexiconStreams.get(blockIndex).readNBytes(nextLexiconEntry[blockIndex], 0, Constants.LEXICON_ENTRY_SIZE);
                        nextTerm[blockIndex].deserializeBinary(nextLexiconEntry[blockIndex]);
                    }
                }
                referenceLexiconTerm.writeToDiskBinary(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }

            mergePartialDocumentTables();


        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    public void mergeBlocksTextual(){

        long start = System.currentTimeMillis();

        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.LEXICON_FILE_PATH + FILE_EXTENSION;

        try (BufferedWriter outputDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter outputFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter outputLexiconStream = new BufferedWriter(new FileWriter(lexiconFile))){

            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            //open all the needed files for each block
            ArrayList<Scanner> lexiconStreams = new ArrayList<>();
            ArrayList<Scanner> postingsDocIdsStreams = new ArrayList<>();
            ArrayList<Scanner> postingsFrequenciesStreams = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconStreams.add(new Scanner(new File(Constants.PARTIAL_LEXICON_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsDocIdsStreams.add(new Scanner(new File(Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsFrequenciesStreams.add(new Scanner(new File(Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + nextBlock + FILE_EXTENSION)));
                nextBlock++;
            }

            //cache in memory of the first terms in the lexicon of each block
            ArrayList<String>[] buffers = new ArrayList[numberOfBlocks];
            for(int i = 0; i < numberOfBlocks; ++i)
                buffers[i] = new ArrayList<>();
            String[] nextLexiconEntry = new String[numberOfBlocks];
            LexiconTerm[] nextTerm = new LexiconTerm[numberOfBlocks];

            ArrayList<Integer> activeBlocks = new ArrayList<>();

            String minTerm = null;

            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                lexiconStreams.get(i).useDelimiter(Pattern.compile("\n"));
                int readLexicons = 0;
                while (lexiconStreams.get(i).hasNext() && readLexicons < Constants.TERMS_TO_CACHE_DURING_MERGE)
                    buffers[i].add(lexiconStreams.get(i).next());

                nextLexiconEntry[i] = buffers[i].remove(0);
                nextTerm[i] = new LexiconTerm();
                nextTerm[i].deserializeTextual(nextLexiconEntry[i]);
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
                    postingsDocIdsStreams.get(blockIndex).useDelimiter(Pattern.compile("\n"));
                    String postingDocIDs = postingsDocIdsStreams.get(blockIndex).next();

                    postingsFrequenciesStreams.get(blockIndex).useDelimiter(Pattern.compile("\n"));
                    String postingFrequencies = postingsFrequenciesStreams.get(blockIndex).next();
                    ArrayList<Integer> docIDs = new ArrayList<>();
                    for(String docIDString: Arrays.asList(postingDocIDs.split(",")))
                        docIDs.add(Integer.parseInt(docIDString));

                    ArrayList<Integer> frequencies = new ArrayList<>();
                    for(String frequencyString: Arrays.asList(postingFrequencies.split(",")))
                        frequencies.add(Integer.parseInt(frequencyString));

                    //merge postings
                    referenceLexiconTerm.extendPostingList(docIDs, frequencies);

                    if (buffers[blockIndex].isEmpty()) {
                        // check if block is finished
                        // consumed those read and nothing more to read for that block
                        if (!lexiconStreams.get(blockIndex).hasNext()) {
                            activeBlocks.remove(blockIndex);
                        } else {
                            //if all the in-memory buffer is consumed, refill it reading again from file
                            lexiconStreams.get(blockIndex).useDelimiter(Pattern.compile("\n"));
                            int readLexicons = 0;
                            while (lexiconStreams.get(blockIndex).hasNext() && readLexicons < Constants.TERMS_TO_CACHE_DURING_MERGE)
                                buffers[blockIndex].add(lexiconStreams.get(blockIndex).next());
                        }
                    } else {
                        nextLexiconEntry[blockIndex] = buffers[blockIndex].remove(0);
                        nextTerm[blockIndex] = new LexiconTerm();
                        nextTerm[blockIndex].deserializeTextual(nextLexiconEntry[blockIndex]);
                    }
                }
                referenceLexiconTerm.writeToDiskTextual(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }
            mergePartialDocumentTables();
            long end = System.currentTimeMillis();
            System.out.println("Merged in " + (end - start) + " ms");
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
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
    }
}
