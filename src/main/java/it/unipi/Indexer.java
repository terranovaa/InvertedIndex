package it.unipi;

import opennlp.tools.stemmer.snowball.SnowballStemmer;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
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
                if(!freeMemory()) {
                    // write to disk
                    // TODO: we avoid sorting since treeMapp do it already, right?
                    writeToDisk("file"+ currentBlock);
                    currentBlock++;
                    // lose references
                    lexicon = null;
                    // use a while loop since we don't know when the garbage collector will come in action
                    while(!freeMemory()) {
                        // hint that suggests the garbage collector to come in action
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        System.gc();
                    }
                }
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
                if(currentId > 5000){
                    break;
                }
            }

            //DEBUG
            for(LexiconTerm lexiconEntry: lexicon.values()){
                //lexiconEntry.printInfo();
            }
        }
        mergeBlocks();
    }

    private boolean freeMemory() {
        // used this solution instead of getRuntime().freeMemory() since of the discrepancy between the memory that the operating system
        // provides the Java Virtual Machine and  the total amount of bytes comprising the chunks of blocks
        // of memory actually being used by the Java Virtual Machine itself.
        // Considering that memory given to Java applications is managed in blocks by the Java Virtual Machine,
        // the amount of free memory available to the Java Virtual Machine may not exactly match the memory
        // available for a Java application.
        long allocatedMemory =
                (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
        long freeMemory = Runtime.getRuntime().maxMemory() - allocatedMemory;
        // the amount is provided in bytes
        System.out.println(freeMemory / (1024*1024));
        if (freeMemory <= MEMORY_THRESHOLD_PERCENTAGE*Runtime.getRuntime().maxMemory())
            return false;
        else return true;
    }

    private String[] tokenize(String document){
        document = document.toLowerCase();
        //remove punctuation and strange characters
        //TODO: Check first option
        document = document.replaceAll("[^\\w\\s]", " ");
        // handle lower case in order to be the same thing
        // TODO: Check second option, remove everything which is not a letter (maybe maintain also numbers?)
        document = document.replaceAll("[^a-zA-Z ]", "").toLowerCase();
        //split in tokens
        // TODO: or \\s+
        return document.split("\\s");
    }

    public void writeToDisk(String filename){
        // TODO: more complex logic? write lexicon and posting lists in different files?
        ObjectOutputStream objStream;
        ByteArrayOutputStream byteStream;
        try {
            byteStream = new ByteArrayOutputStream();
            objStream = new ObjectOutputStream(byteStream);;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
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
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
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
        WeakReference ref = new WeakReference<Object>(obj);
        obj = null;
        while(ref.get() != null) {
            System.gc();
        }
    }
}
