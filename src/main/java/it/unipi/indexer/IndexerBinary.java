package it.unipi.indexer;

import it.unipi.models.CollectionStatistics;
import it.unipi.models.Document;
import it.unipi.models.LexiconTermBinaryIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.DiskDataStructuresSearch;
import it.unipi.utils.EncodingUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexerBinary extends Indexer<LexiconTermBinaryIndexing> {
    public IndexerBinary() {
        super(LexiconTermBinaryIndexing::new, Constants.DAT_FORMAT);
    }

    @Override
    protected void writeToDisk(){

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
            for (Map.Entry<String, LexiconTermBinaryIndexing> entry : lexicon.entrySet()) {
                LexiconTermBinaryIndexing lexiconTerm = entry.getValue();
                lexiconTerm.setDocIdsOffset(docIDsFileOffset);
                lexiconTerm.setFrequenciesOffset(frequenciesFileOffset);
                // docIDs
                List<Integer> docIDs = lexiconTerm.getPostingListDocIds();
                byte[] encodedDocIDs = EncodingUtils.encode(docIDs);
                docIDsFileOffset += encodedDocIDs.length;
                lexiconTerm.setDocIdsSize(encodedDocIDs.length);
                postingsDocIdsStream.write(encodedDocIDs);
                // frequencies
                List<Integer> frequencies = lexiconTerm.getPostingListFrequencies();
                byte[] encodedFrequencies = EncodingUtils.encode(frequencies);
                frequenciesFileOffset += encodedFrequencies.length;
                lexiconTerm.setFrequenciesSize(encodedFrequencies.length);
                postingsFrequenciesStream.write(encodedFrequencies);
                // lexicon
                byte[] lexiconEntry = lexiconTerm.serialize();
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

    @Override
    public void merge(){
        String postingsDocIdsFile = Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION;
        String postingsFrequenciesFile = Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION;
        String lexiconFile = Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION;

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
            LexiconTermBinaryIndexing[] nextTerm = new LexiconTermBinaryIndexing[numberOfBlocks];
            //used to keep track of how many bytes were read the last time
            int[] bytesRead = new int[numberOfBlocks];
            ArrayList<Integer> activeBlocks = new ArrayList<>();

            for(int i=0; i < numberOfBlocks; i++){
                activeBlocks.add(i);
                //read from file
                bytesRead[i] = lexiconStreams.get(i).readNBytes(nextLexiconEntry[i], 0,Constants.LEXICON_ENTRY_SIZE);
                nextTerm[i] = new LexiconTermBinaryIndexing();
                nextTerm[i].deserialize(nextLexiconEntry[i]);
            }

            while(activeBlocks.size() > 0){

                List<Integer> lexiconsToMerge = getLexiconsToMerge(activeBlocks, nextTerm);

                //create a new lexiconTerm object for the min term
                LexiconTermBinaryIndexing referenceLexiconTerm = new LexiconTermBinaryIndexing(nextTerm[lexiconsToMerge.get(0)].getTerm());

                //merge everything
                for (Integer blockIndex: lexiconsToMerge){
                    LexiconTermBinaryIndexing nextBlockToMerge = nextTerm[blockIndex];

                    //merge statistics
                    //referenceLexiconTerm.setDocumentFrequency(referenceLexiconTerm.getDocumentFrequency() + nextBlockToMerge.getDocumentFrequency());
                    //referenceLexiconTerm.setCollectionFrequency(referenceLexiconTerm.getCollectionFrequency() + nextBlockToMerge.getCollectionFrequency());

                    //get posting list from disk
                    byte[] postingDocIDs = postingsDocIdsStreams.get(blockIndex).readNBytes(nextBlockToMerge.getDocIdsSize());
                    byte[] postingFrequencies = postingsFrequenciesStreams.get(blockIndex).readNBytes(nextBlockToMerge.getFrequenciesSize());
                    referenceLexiconTerm.mergeEncodedPostings(postingDocIDs, postingFrequencies);

                    bytesRead[blockIndex] = lexiconStreams.get(blockIndex).readNBytes(nextLexiconEntry[blockIndex], 0, Constants.LEXICON_ENTRY_SIZE);
                    nextTerm[blockIndex].deserialize(nextLexiconEntry[blockIndex]);
                    if(bytesRead[blockIndex] < Constants.LEXICON_ENTRY_SIZE){
                        activeBlocks.remove(blockIndex);
                    }
                }
                referenceLexiconTerm.writeToDisk(outputDocIdsStream, outputFrequenciesStream, outputLexiconStream);
            }

            mergePartialDocumentTables();
            outputDocIdsStream.close();
            outputFrequenciesStream.close();
            outputLexiconStream.close();

            try (FileOutputStream fosCollectionStatistics = new FileOutputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)){
                fosCollectionStatistics.write(collectionStatistics.serializeBinary());
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    public void refineIndex() {
        //take merged lexicon and compute term statistics
        try (OutputStream outLexiconStream = new BufferedOutputStream(new FileOutputStream(Constants.LEXICON_FILE_PATH + FILE_EXTENSION));
            InputStream inLexiconStream = new BufferedInputStream(new FileInputStream(Constants.MERGED_LEXICON_FILE_PATH + FILE_EXTENSION));
            InputStream inDocIdStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + FILE_EXTENSION));
            InputStream inFrequencyStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + FILE_EXTENSION))
        )
        {

            //load collection statistics and open document table
            CollectionStatistics cs = DiskDataStructuresSearch.readCollectionStatistics();
            FileChannel docTableChannel = FileChannel.open(Paths.get(Constants.DOCUMENT_TABLE_FILE_PATH + Constants.DAT_FORMAT));
            MappedByteBuffer docTableBuffer = docTableChannel.map(FileChannel.MapMode.READ_ONLY, 0, docTableChannel.size()).load();

            byte[] buffer = new byte[Constants.LEXICON_ENTRY_SIZE];
            int bytesRead = inLexiconStream.read(buffer);

            //read each entry from the lexicon
            while(bytesRead == Constants.LEXICON_ENTRY_SIZE){
                LexiconTermBinaryIndexing entry = new LexiconTermBinaryIndexing();
                entry.deserializeBinary(buffer);

                //jump over skip pointers if any
                int dimSkipPointers = 0;
                if (entry.getDocumentFrequency() > Constants.SKIP_POINTERS_THRESHOLD) {
                    int blockSize = (int) Math.ceil(Math.sqrt(entry.getDocumentFrequency()));
                    int numSkipBlocks = (int) Math.ceil((double)entry.getDocumentFrequency() / (double)blockSize);
                    dimSkipPointers = 20 * (numSkipBlocks-1);
                    //FileInputStream skip method doesn't work ¯\_(ツ)_/¯
                    byte[] skip = new byte[dimSkipPointers];
                    inDocIdStream.read(skip);
                }

                //get posting lists
                byte[] postingDocIDs = inDocIdStream.readNBytes(entry.getDocIdsSize() - dimSkipPointers);
                byte[] postingFrequencies = inFrequencyStream.readNBytes(entry.getFrequenciesSize());
                entry.mergeEncodedPostings(postingDocIDs, postingFrequencies);

                //compute term upper bound and collection frequency
                entry.computeStatistics(docTableBuffer, cs);

                //TODO could change only relevant bytes inside previously read buffer instead of re-serializing everything?
                //write to definitive lexicon file
                outLexiconStream.write(entry.serialize());
                bytesRead = inLexiconStream.read(buffer);

                //posting lists not needed, to save memory we remove them (we are keeping the terms with the larger posting lists)
                entry.setPostingListDocIds(null);
                entry.setPostingListFrequencies(null);
            }
        } catch (IOException ee) {
            ee.printStackTrace();
        }
    }

}
