package it.unipi.indexer;

import it.unipi.models.Document;
import it.unipi.models.LexiconTermBinaryIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.EncodingUtils;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BinaryIndexer extends Indexer<LexiconTermBinaryIndexing> {
    public BinaryIndexer() throws ConfigurationException, IOException {
        super(LexiconTermBinaryIndexing::new, Constants.DAT_FORMAT);
    }

    // function that writes to disk the partial data structures
    @Override
    protected void writeBlockToDisk(){

        // partial file paths
        String postingsDocIdsFile = Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + currentBlock + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + currentBlock + FILE_EXTENSION;
        String lexiconFile = Constants.PARTIAL_LEXICON_FILE_PATH + currentBlock + FILE_EXTENSION;
        String documentTableFile = Constants.PARTIAL_DOCUMENT_TABLE_FILE_PATH + currentBlock + FILE_EXTENSION;

        // offsets used for saving the start of a posting list for each term
        int docIDsFileOffset = 0;
        int frequenciesFileOffset = 0;

        long start = System.currentTimeMillis();

        // opening the output streams with try-with-resources
        try (OutputStream postingsDocIdsStream = new BufferedOutputStream(new FileOutputStream(postingsDocIdsFile));
             OutputStream postingsFrequenciesStream = new BufferedOutputStream(new FileOutputStream(postingsFrequenciesFile));
             OutputStream lexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));
             OutputStream documentTableStream = new BufferedOutputStream(new FileOutputStream(documentTableFile))
        ) {
            // looping over the lexicon
            for (Map.Entry<String, LexiconTermBinaryIndexing> entry : lexicon.entrySet()) {
                LexiconTermBinaryIndexing lexiconTerm = entry.getValue();
                //saving the offsets of the posting list relative to the term
                lexiconTerm.setDocIdsOffset(docIDsFileOffset);
                lexiconTerm.setFrequenciesOffset(frequenciesFileOffset);

                // docIDs posting list
                List<Integer> docIDs = lexiconTerm.getPostingListDocIds();
                // encoding the docIds with VariableByte encoding
                //byte[] encodedDocIDs = EncodingUtils.encode(docIDs);
                byte[] encodedDocIDs = EncodingUtils.intListToByteArray(docIDs);
                // moving the offset for the next posting list
                docIDsFileOffset += encodedDocIDs.length;
                // setting the size of the term's docIds posting list in bytes
                lexiconTerm.setDocIdsSize(encodedDocIDs.length);
                postingsDocIdsStream.write(encodedDocIDs);

                // frequencies posting list
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                // encoding the frequencies with VariableByte encoding
                //byte[] encodedFrequencies = EncodingUtils.encode(frequencies);
                byte[] encodedFrequencies = EncodingUtils.intListToByteArray(frequencies);
                // moving the offset for the next posting list
                frequenciesFileOffset += encodedFrequencies.length;
                // setting the size of the term's frequencies posting list in bytes
                lexiconTerm.setFrequenciesSize(encodedFrequencies.length);
                postingsFrequenciesStream.write(encodedFrequencies);

                // serializing and writing to file the lexicon entry
                byte[] lexiconEntry = lexiconTerm.serialize();
                lexiconStream.write(lexiconEntry);
            }

            // serializing and writing to file the doc table
            for (Map.Entry<Integer, Document> doc : documentTable.entrySet()) {
                byte[] documentTableEntry = doc.getValue().serializeBinary();
                documentTableStream.write(documentTableEntry);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("Copied in " + (end - start) + " ms");
    }

    // function that merges the partial data structures and writes the merged files to disk
    @Override
    public void mergeBlocks(){

        // merging the doc table
        try {
            mergePartialDocumentTables();
        } catch (IOException ioe){
            ioe.printStackTrace();
        }

        // file paths
        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.LEXICON_FILE_PATH + FILE_EXTENSION;

        try {
            FileOutputStream outputDocIdsStream = new FileOutputStream(postingsDocIdsFile);
            FileOutputStream outputFrequenciesStream = new FileOutputStream(postingsFrequenciesFile);
            OutputStream outputLexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));

            // loading the document table for term upper bound computation
            FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
            MappedByteBuffer docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();

            // number of partial files to read from
            int numberOfBlocks = currentBlock + 1;
            int nextBlock = 0;

            // opening all the partial files
            ArrayList<InputStream> lexiconStreams = new ArrayList<>();
            ArrayList<InputStream> postingsDocIdsStreams = new ArrayList<>();
            ArrayList<InputStream> postingsFrequenciesStreams = new ArrayList<>();
            while(nextBlock < numberOfBlocks){
                lexiconStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_LEXICON_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsDocIdsStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_POSTINGS_DOC_IDS_FILE_PATH + nextBlock + FILE_EXTENSION)));
                postingsFrequenciesStreams.add(new BufferedInputStream(new FileInputStream(Constants.PARTIAL_POSTINGS_FREQUENCIES_FILE_PATH + nextBlock + FILE_EXTENSION)));
                nextBlock++;
            }

            // 2D array containing one serialized term for each block
            byte[][] nextLexiconEntry = new byte[numberOfBlocks][Constants.LEXICON_ENTRY_SIZE];
            // array of deserialized lexicon terms
            LexiconTermBinaryIndexing[] nextTerm = new LexiconTermBinaryIndexing[numberOfBlocks];
            // used to keep track of how many bytes were read the last time by each block
            int[] bytesRead = new int[numberOfBlocks];
            // used to keep track of unfinished blocks
            ArrayList<Integer> activeBlocks = new ArrayList<>();

            // deserializing the first entry for each block
            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                bytesRead[i] = lexiconStreams.get(i).readNBytes(nextLexiconEntry[i], 0,Constants.LEXICON_ENTRY_SIZE);
                nextTerm[i] = new LexiconTermBinaryIndexing();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
            }

            while(activeBlocks.size() > 0){

                // getting the indexes of the blocks containing the minimum term in lexicographical order
                List<Integer> blocksToMerge = getBlocksToMerge(activeBlocks, nextTerm);

                // creating a new lexiconTerm object for the min term
                LexiconTermBinaryIndexing referenceLexiconTerm = new LexiconTermBinaryIndexing(nextTerm[blocksToMerge.get(0)].getTerm());

                // merging the encoded posting lists
                for (Integer blockIndex: blocksToMerge){

                    //get partial information about the term stored in the current block
                    LexiconTermBinaryIndexing nextBlockToMerge = nextTerm[blockIndex];

                    //merge document frequencies
                    referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());

                    // reading and merging the corresponding posting lists from disk
                    byte[] postingDocIDs = postingsDocIdsStreams.get(blockIndex).readNBytes(nextBlockToMerge.getDocIdsSize());
                    byte[] postingFrequencies = postingsFrequenciesStreams.get(blockIndex).readNBytes(nextBlockToMerge.getFrequenciesSize());
                    referenceLexiconTerm.getPostingListDocIds().addAll(EncodingUtils.byteArrayToIntList(postingDocIDs));
                    referenceLexiconTerm.getPostingListFrequencies().addAll(EncodingUtils.byteArrayToIntList(postingFrequencies));

                    // reading  and deserializing the next term entry from file
                    bytesRead[blockIndex] = lexiconStreams.get(blockIndex).readNBytes(nextLexiconEntry[blockIndex], 0, Constants.LEXICON_ENTRY_SIZE);
                    nextTerm[blockIndex].deserialize(nextLexiconEntry[blockIndex]);

                    // if the lexicon file is finished, we remove the block from the active ones because the block contains no more terms
                    if(bytesRead[blockIndex] < Constants.LEXICON_ENTRY_SIZE){
                        activeBlocks.remove(blockIndex);
                        // closing the corresponding fileInputStreams
                        lexiconStreams.get(blockIndex).close();
                        postingsDocIdsStreams.get(blockIndex).close();
                        postingsFrequenciesStreams.get(blockIndex).close();
                    }
                }

                // computing term upper bound and collection frequency
                referenceLexiconTerm.computeStatistics(docTableBuffer, collectionStatistics);

                // gaps implementation
                int previousDocId = -1;
                int currentDocIdGap;
                //ArrayList<Integer> postingListDocIds = EncodingUtils.decode(referenceLexiconTerm.getEncodedDocIDs());
                ArrayList<Integer> postingListDocIds = referenceLexiconTerm.getPostingListDocIds();
                ArrayList<Integer> postingListDocIdGaps = new ArrayList<>(postingListDocIds.size());
                for(int currentDocId : postingListDocIds){
                    currentDocIdGap = currentDocId;
                    if(previousDocId != -1)  // not the head of the list
                        currentDocIdGap -= previousDocId;
                    previousDocId = currentDocId;
                    postingListDocIdGaps.add(currentDocIdGap);
                }

                referenceLexiconTerm.setPostingListDocIds(postingListDocIdGaps);

                // writing the term to the lexicon and the merged posting lists to the inverted index
                // old doc ids used only for defining the key of the skip pointer
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream, postingListDocIds);
            }

            outputDocIdsStream.close();
            outputFrequenciesStream.close();
            outputLexiconStream.close();

            // writing collection statistics to file
            try (FileOutputStream fosCollectionStatistics = new FileOutputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)){
                fosCollectionStatistics.write(collectionStatistics.serializeBinary());
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }
}
