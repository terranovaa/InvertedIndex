package it.unipi.indexer;

import it.unipi.models.*;
import it.unipi.utils.Constants;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.EncodingUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class BinaryIndexer extends Indexer<LexiconTermBinaryIndexing> {
    public BinaryIndexer() {
        super(LexiconTermBinaryIndexing::new, Constants.DAT_FORMAT);
    }

    // function that writes to disk the partial data structures
    @Override
    protected void writeToDisk(){

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
                byte[] encodedDocIDs = EncodingUtils.encode(docIDs);
                // moving the offset for the next posting list
                docIDsFileOffset += encodedDocIDs.length;
                // setting the size of the term's docIds posting list in bytes
                lexiconTerm.setDocIdsSize(encodedDocIDs.length);
                postingsDocIdsStream.write(encodedDocIDs);

                // frequencies posting list
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                // encoding the frequencies with VariableByte encoding
                byte[] encodedFrequencies = EncodingUtils.encode(frequencies);
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
    public void merge(){

        // file paths
        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION;

        try {
            FileOutputStream outputDocIdsStream = new FileOutputStream(postingsDocIdsFile);
            FileOutputStream outputFrequenciesStream = new FileOutputStream(postingsFrequenciesFile);
            OutputStream outputLexiconStream = new BufferedOutputStream(new FileOutputStream(lexiconFile));

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
                List<Integer> lexiconsToMerge = getLexiconsToMerge(activeBlocks, nextTerm);

                // creating a new lexiconTerm object for the min term
                LexiconTermBinaryIndexing referenceLexiconTerm = new LexiconTermBinaryIndexing(nextTerm[lexiconsToMerge.get(0)].getTerm());

                // merging the encoded posting lists
                for (Integer blockIndex: lexiconsToMerge){

                    LexiconTermBinaryIndexing nextBlockToMerge = nextTerm[blockIndex];

                    referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());

                    // reading and merging the corresponding posting lists from disk
                    byte[] postingDocIDs = postingsDocIdsStreams.get(blockIndex).readNBytes(nextBlockToMerge.getDocIdsSize());
                    byte[] postingFrequencies = postingsFrequenciesStreams.get(blockIndex).readNBytes(nextBlockToMerge.getFrequenciesSize());
                    referenceLexiconTerm.mergeEncodedPostings(postingDocIDs, postingFrequencies);

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

                // writing the term to the lexicon and the merged posting lists to the inverted index
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }

            // merging the doc table
            mergePartialDocumentTables();
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

    // function used for computing the term upper bound for each term
    public void refineIndex() {

        try (OutputStream outLexiconStream = new BufferedOutputStream(new FileOutputStream(Constants.LEXICON_FILE_PATH + FILE_EXTENSION));
            InputStream inLexiconStream = new BufferedInputStream(new FileInputStream(Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION));
             OutputStream outNewDocIdStream = new BufferedOutputStream(new FileOutputStream(Constants.POSTINGS_DOC_IDS_GAPS_FILE_PATH + FILE_EXTENSION));
             InputStream inDocIdStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION));
            InputStream inFrequencyStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION))
        )
        {
            // loading the collection statistics and the document table
            CollectionStatistics cs = DiskDataStructuresSearch.readCollectionStatistics();
            FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
            MappedByteBuffer docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();

            byte[] buffer = new byte[Constants.LEXICON_ENTRY_SIZE];
            int bytesRead = inLexiconStream.read(buffer);
            LexiconTermBinaryIndexing.docIDsFileOffset = 0;
            LexiconTermBinaryIndexing.frequenciesFileOffset = 0;
            // for each term
            while(bytesRead == Constants.LEXICON_ENTRY_SIZE) {

                LexiconTermBinaryIndexing entry = new LexiconTermBinaryIndexing();
                entry.deserializeBinary(buffer);

                // getting posting lists
                byte[] postingDocIDs = inDocIdStream.readNBytes(entry.getDocIdsSize());
                byte[] postingFrequencies = inFrequencyStream.readNBytes(entry.getFrequenciesSize());

                // gaps implementation
                int previousDocID = -1;
                int newCurrentDocID;
                ArrayList<Integer> postingListNewDocIds = new ArrayList<>();
                ArrayList<Integer> postingListOldDocIds = EncodingUtils.decode(postingDocIDs);
                byte[] newEncoding;
                for(int completeDocID: postingListOldDocIds){
                    newCurrentDocID = completeDocID;
                    if(previousDocID != -1)  // not the head of the list
                        newCurrentDocID -= previousDocID;
                    previousDocID = completeDocID;
                    postingListNewDocIds.add(newCurrentDocID);
                }

                newEncoding = EncodingUtils.encode(postingListNewDocIds);

                // old doc ids used only for defining the key of the skip pointer
                entry.setEncodedPostings(newEncoding, postingFrequencies);
                entry.writeRefinedPostingListToDisk(outNewDocIdStream, postingListOldDocIds);

                // computing term upper bound and collection frequency
                entry.computeStatistics(docTableBuffer, cs);

                // writing to definitive lexicon file
                outLexiconStream.write(entry.serialize());
                bytesRead = inLexiconStream.read(buffer);
            }

        } catch (IOException ee) {
            ee.printStackTrace();
        }

        Path source = Paths.get(Constants.POSTINGS_DOC_IDS_GAPS_FILE_PATH + FILE_EXTENSION);

        try{
            // rename a file in the same directory
            Files.move(source, source.resolveSibling("postings_doc_ids.dat"), StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
