package it.unipi.utils;

import it.unipi.models.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;

public class DiskDataStructuresSearch {
    public static CollectionStatistics readCollectionStatistics(){
        CollectionStatistics collectionStatistics = new CollectionStatistics();
        try (FileInputStream fisCollectionStatistics = new FileInputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)){
            byte[] csBytes = fisCollectionStatistics.readNBytes(12);
            collectionStatistics.deserializeBinary(csBytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return collectionStatistics;
    }

    // retrieves a Document from the doc table given a docId
    public static Document docTableDiskSearch(int docId, MappedByteBuffer docTableBuffer) {
        Document doc = new Document();
        int fileSeekPointer = docId * Constants.DOCUMENT_ENTRY_SIZE;
        docTableBuffer.position(fileSeekPointer);
        byte[] result = new byte[Constants.DOCUMENT_ENTRY_SIZE];
        docTableBuffer.get(result, 0, Constants.DOCUMENT_ENTRY_SIZE);
        doc.deserializeBinary(result);
        return doc;
    }

    // retrieves a term from the lexicon using binary search
    public static LexiconTerm lexiconDiskSearch(String term, int numberOfTerms, MappedByteBuffer lexiconBuffer) {
        int pointer;

        LexiconTerm currentEntry = new LexiconTerm();
        int leftExtreme = 0;
        int rightExtreme = numberOfTerms;

        while(rightExtreme > leftExtreme){
            pointer = (leftExtreme + ((rightExtreme - leftExtreme) / 2)) * Constants.LEXICON_ENTRY_SIZE;
            lexiconBuffer.position(pointer);
            byte[] buffer = new byte[Constants.LEXICON_ENTRY_SIZE];
            lexiconBuffer.get(buffer, 0, Constants.LEXICON_ENTRY_SIZE);
            String currentTerm = currentEntry.deserializeTerm(buffer);
            if(currentTerm.compareTo(term) > 0){
                //we go left on the array
                rightExtreme = rightExtreme - (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
            } else if (currentTerm.compareTo(term) < 0) {
                //we go right on the array
                leftExtreme = leftExtreme + (int)Math.ceil(((double)(rightExtreme - leftExtreme) / 2));
            } else {
                currentEntry.deserializeBinary(buffer);
                return currentEntry;
            }
        }
        return null;
    }

    public static LexiconTerm lexiconDiskSearch(int termIndex, int numberOfTerms, MappedByteBuffer lexiconBuffer) {

        LexiconTerm currentEntry = new LexiconTerm();
        lexiconBuffer.position(termIndex * Constants.LEXICON_ENTRY_SIZE);
        byte[] buffer = new byte[Constants.LEXICON_ENTRY_SIZE];
        lexiconBuffer.get(buffer, 0, Constants.LEXICON_ENTRY_SIZE);
        currentEntry.deserializeBinary(buffer);
        return currentEntry;
    }
}
