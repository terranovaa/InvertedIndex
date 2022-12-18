package it.unipi.indexer;

import it.unipi.models.Document;
import it.unipi.models.LexiconTermTextualIndexing;
import it.unipi.utils.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexerTextual extends Indexer<LexiconTermTextualIndexing> {

    public IndexerTextual() {
        super(LexiconTermTextualIndexing::new, Constants.TXT_FORMAT);
    }

    @Override
    protected void writeToDisk(){
        System.out.println("Writing to disk in textual format..");
        String postingsDocIdsFile = Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + currentBlock + FILE_EXTENSION.toLowerCase();
        String postingsFrequenciesFile = Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + currentBlock + FILE_EXTENSION.toLowerCase();
        String lexiconFile = Constants.PARTIAL_LEXICON_FILE_PATH + currentBlock + FILE_EXTENSION.toLowerCase();
        String documentTableFile = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + currentBlock + FILE_EXTENSION.toLowerCase();
        String documentTableFileSplit1 = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + "_SPLIT1_" + currentBlock + FILE_EXTENSION.toLowerCase();
        String documentTableFileSplit2 = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + "_SPLIT2_" + currentBlock + FILE_EXTENSION.toLowerCase();

        long start = System.currentTimeMillis();
        try (BufferedWriter postingsDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter postingsFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter lexiconStream = new BufferedWriter(new FileWriter(lexiconFile));
             BufferedWriter documentTableStream = new BufferedWriter(new FileWriter(documentTableFile));
             BufferedWriter documentTableStreamSplit1 = new BufferedWriter(new FileWriter(documentTableFileSplit1));
             BufferedWriter documentTableStreamSplit2 = new BufferedWriter(new FileWriter(documentTableFileSplit2))
        ) {
            for (Map.Entry<String, LexiconTermTextualIndexing> entry : lexicon.entrySet()) {
                LexiconTermTextualIndexing lexiconTerm = entry.getValue();
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
                String[] lexiconEntry = lexiconTerm.serialize();
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
            // TEST SPLIT
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                String[][] documentTableEntry = doc.getValue().serializeTextualSplit();
                for(int i = 0; i < documentTableEntry[0].length; ++i)
                    if(i != documentTableEntry[0].length-1)
                        documentTableStreamSplit1.write(documentTableEntry[0][i]+",");
                    else documentTableStreamSplit1.write(documentTableEntry[0][i]+"\n"); // since we don't have the offset information here, we use \n as delimiter
                for(int i = 0; i < documentTableEntry[1].length; ++i)
                    if(i != documentTableEntry[1].length-1)
                        documentTableStreamSplit2.write(documentTableEntry[1][i]+",");
                    else documentTableStreamSplit2.write(documentTableEntry[1][i]+"\n"); // since we don't have the offset information here, we use \n as delimiter
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

    @Override
    public void merge(){

        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION.toLowerCase();
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION.toLowerCase();
        String lexiconFile = Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION.toLowerCase();

        try (BufferedWriter outputDocIdsStream = new BufferedWriter(new FileWriter(postingsDocIdsFile));
             BufferedWriter outputFrequenciesStream = new BufferedWriter(new FileWriter(postingsFrequenciesFile));
             BufferedWriter outputLexiconStream = new BufferedWriter(new FileWriter(lexiconFile))){

            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            //open all the needed files for each block
            ArrayList<BufferedReader> lexiconReader = new ArrayList<>();
            ArrayList<BufferedReader> postingsDocIdsReader = new ArrayList<>();
            ArrayList<BufferedReader> postingsFrequenciesReader = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_LEXICON_FILE_PATH + nextBlock + FILE_EXTENSION.toLowerCase()))));
                postingsDocIdsReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + nextBlock + FILE_EXTENSION.toLowerCase()))));
                postingsFrequenciesReader.add(new BufferedReader(new InputStreamReader(new FileInputStream(Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + nextBlock + FILE_EXTENSION.toLowerCase()))));
                nextBlock++;
            }

            String[] nextLexiconEntry = new String[numberOfBlocks];
            LexiconTermTextualIndexing[] nextTerm = new LexiconTermTextualIndexing[numberOfBlocks];

            ArrayList<Integer> activeBlocks = new ArrayList<>();

            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                nextLexiconEntry[i] = lexiconReader.get(i).readLine();
                nextTerm[i] = new LexiconTermTextualIndexing();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
            }

            while(activeBlocks.size() > 0){

                List<Integer> lexiconsToMerge = getLexiconsToMerge(activeBlocks, nextTerm);

                //create a new lexiconTerm object for the min term
                LexiconTermTextualIndexing referenceLexiconTerm = new LexiconTermTextualIndexing(nextTerm[lexiconsToMerge.get(0)].getTerm());
                //merge everything
                for (Integer blockIndex: lexiconsToMerge){
                    LexiconTermTextualIndexing nextBlockToMerge = nextTerm[blockIndex];

                    //merge statistics
                    referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());
                    referenceLexiconTerm.setCollectionFrequency(referenceLexiconTerm.getCollectionFrequency() + nextBlockToMerge.getCollectionFrequency());

                    //get posting list from disk
                    String postingDocIDs = postingsDocIdsReader.get(blockIndex).readLine();
                    String postingFrequencies = postingsFrequenciesReader.get(blockIndex).readLine();
                    ArrayList<Integer> docIDs = new ArrayList<>();
                    for(String docIDString: postingDocIDs.split(","))
                        docIDs.add(Integer.parseInt(docIDString));

                    ArrayList<Integer> frequencies = new ArrayList<>();
                    for(String frequencyString: postingFrequencies.split(","))
                        frequencies.add(Integer.parseInt(frequencyString));

                    //merge postings
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
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }
            mergePartialDocumentTables();
            // TEST
            mergePartialDocumentTablesSplit();

            try (BufferedWriter bwCollectionStatistics = new BufferedWriter(new FileWriter(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.TXT_FORMAT))) {
                bwCollectionStatistics.write(collectionStatistics.serializeToString());
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    @Override
    public void refineIndex() {
    }
}
