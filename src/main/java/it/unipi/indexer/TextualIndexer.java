package it.unipi.indexer;

import it.unipi.models.Document;
import it.unipi.models.LexiconTermTextualIndexing;
import it.unipi.utils.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// This class was used when we were building the index just to check if everything worked
public class TextualIndexer extends Indexer<LexiconTermTextualIndexing> {

    public TextualIndexer() {
        super(LexiconTermTextualIndexing::new, Constants.TXT_FORMAT);
    }

    @Override
    protected void writeToDisk(){

        // partial file paths
        String postingsDocIdsFile = Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + currentBlock + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + currentBlock + FILE_EXTENSION;
        String lexiconFile = Constants.PARTIAL_LEXICON_FILE_PATH + currentBlock + FILE_EXTENSION;
        String documentTableFile = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + currentBlock + FILE_EXTENSION;

        long start = System.currentTimeMillis();

        // opening the buffered writers with try-with-resources
        try (BufferedWriter postingsDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter postingsFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter lexiconStream = new BufferedWriter(new FileWriter(lexiconFile));
             BufferedWriter documentTableStream = new BufferedWriter(new FileWriter(documentTableFile))
        ) {
            // looping over the lexicon
            for (Map.Entry<String, LexiconTermTextualIndexing> entry : lexicon.entrySet()) {
                LexiconTermTextualIndexing lexiconTerm = entry.getValue();

                // docIDs posting list
                List<Integer> docIDs = lexiconTerm.getPostingListDocIds();
                for(int i = 0; i < docIDs.size(); i++) {
                    if (i != docIDs.size() - 1)
                        postingsDocIdsStream.write(docIDs.get(i).toString() + ","); // doc ids are separated by ,
                    else
                        postingsDocIdsStream.write(docIDs.get(i).toString() + "\n"); // posting lists are separated by \n
                }

                // frequencies posting list
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                for(int i = 0; i < frequencies.size(); ++i) {
                    if (i != docIDs.size() - 1)
                        postingsFrequenciesStream.write(frequencies.get(i).toString() + ",");
                    else postingsFrequenciesStream.write(frequencies.get(i).toString() + "\n");
                }

                // writing to file the lexicon entry
                String[] lexiconEntry = lexiconTerm.serialize();
                for(int i = 0; i < lexiconEntry.length; ++i) {
                    if (i != lexiconEntry.length - 1)
                        lexiconStream.write(lexiconEntry[i] + ",");
                    else lexiconStream.write(lexiconEntry[i] + "\n");
                }
            }

            // writing to file the doc table
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                String[] documentTableEntry = doc.getValue().serializeTextual();
                for(int i = 0; i < documentTableEntry.length; ++i)
                    if(i != documentTableEntry.length-1)
                        documentTableStream.write(documentTableEntry[i]+",");
                    else documentTableStream.write(documentTableEntry[i]+"\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("Partial files written in " + (end - start) + " ms");
    }

    // function that merges the partial data structures and writes the merged files to disk
    @Override
    public void merge(){

        // file paths
        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION;

        try (BufferedWriter outputDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter outputFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter outputLexiconStream = new BufferedWriter(new FileWriter(lexiconFile))){

            // number of partial files to read from
            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            // opening all the partial files
            ArrayList<BufferedReader> lexiconReader = new ArrayList<>();
            ArrayList<BufferedReader> postingsDocIdsReader = new ArrayList<>();
            ArrayList<BufferedReader> postingsFrequenciesReader = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_LEXICON_FILE_PATH + nextBlock + FILE_EXTENSION))));
                postingsDocIdsReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + nextBlock + FILE_EXTENSION))));
                postingsFrequenciesReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + nextBlock + FILE_EXTENSION))));
                nextBlock++;
            }

            // array containing one serialized term for each block
            String[] nextLexiconEntry = new String[numberOfBlocks];
            // array of deserialized lexicon terms
            LexiconTermTextualIndexing[] nextTerm = new LexiconTermTextualIndexing[numberOfBlocks];

            // used to keep track of unfinished blocks
            ArrayList<Integer> activeBlocks = new ArrayList<>();

            // deserializing the first entry for each block
            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                nextLexiconEntry[i] = lexiconReader.get(i).readLine();
                nextTerm[i] = new LexiconTermTextualIndexing();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
            }

            while(activeBlocks.size() > 0){

                // getting the indexes of the blocks containing the minimum term in lexicographical order
                List<Integer> lexiconsToMerge = getLexiconsToMerge(activeBlocks, nextTerm);

                // creating a new lexiconTerm object for the min term
                LexiconTermTextualIndexing referenceLexiconTerm = new LexiconTermTextualIndexing(nextTerm[lexiconsToMerge.get(0)].getTerm());

                //merging everything
                for (Integer blockIndex: lexiconsToMerge){

                    LexiconTermTextualIndexing nextBlockToMerge = nextTerm[blockIndex];

                    // merging statistics
                    referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());
                    referenceLexiconTerm.setCollectionFrequency(referenceLexiconTerm.getCollectionFrequency() + nextBlockToMerge.getCollectionFrequency());

                    //getting posting list from disk
                    String postingDocIDs = postingsDocIdsReader.get(blockIndex).readLine();
                    String postingFrequencies = postingsFrequenciesReader.get(blockIndex).readLine();
                    ArrayList<Integer> docIDs = new ArrayList<>();
                    for(String docIDString: postingDocIDs.split(","))
                        docIDs.add(Integer.parseInt(docIDString));

                    ArrayList<Integer> frequencies = new ArrayList<>();
                    for(String frequencyString: postingFrequencies.split(","))
                        frequencies.add(Integer.parseInt(frequencyString));

                    // merging postings
                    referenceLexiconTerm.extendPostingList(docIDs, frequencies);

                    if(activeBlocks.contains(blockIndex)){
                        String nextLine = lexiconReader.get(blockIndex).readLine();
                        if (nextLine == null) {
                            activeBlocks.remove(blockIndex);
                        } else {
                            nextTerm[blockIndex] = new LexiconTermTextualIndexing();
                            nextTerm[blockIndex].deserialize(nextLine);
                        }
                    }
                }

                // writing the term to the lexicon and the merged posting lists to the inverted index
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }
            // merging the doc table
            mergePartialDocumentTables();
            try (BufferedWriter bwCollectionStatistics = new BufferedWriter(new FileWriter(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.TXT_FORMAT))) {
                bwCollectionStatistics.write(collectionStatistics.serializeToString());
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }
}
