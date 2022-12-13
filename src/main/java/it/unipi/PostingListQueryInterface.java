package it.unipi;



import it.unipi.exceptions.TerminatedListException;
import it.unipi.models.LexiconTermIndexing;
import it.unipi.utils.Constants;
import it.unipi.utils.Utils;

import java.io.*;
import java.util.ArrayList;

// feel free to change the class name, I know you will not like it
public class PostingListQueryInterface {
    String term;
    int docIDsFileOffset;
    int docIDIncrement;
    int docIDMaxSize;
    int frequenciesFileOffset;
    int frequenciesIncrement;
    int freqMaxSize;
    InputStream postingsDocIdStream;
    InputStream postingsFrequenciesStream;
    int currentDocID;
    int currentFreq;

    public PostingListQueryInterface(LexiconTermIndexing lexiconTermIndexing) {
        docIDsFileOffset = lexiconTermIndexing.getDocIdsOffset();
        docIDMaxSize = lexiconTermIndexing.getDocIdsSize();
        frequenciesFileOffset = lexiconTermIndexing.getFrequenciesOffset();
        freqMaxSize = lexiconTermIndexing.getFrequenciesSize();
        term = lexiconTermIndexing.getTerm();
    }

    public void openList(){
        try {
            // TODO: se apriamo così lo stream, rischiamo che cacha i primi bytes? I think no
            frequenciesIncrement = 0;
            docIDIncrement = 0;
            postingsDocIdStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_DOC_IDS_FILE_PATH + Constants.DAT_FORMAT));
            postingsFrequenciesStream = new BufferedInputStream(new FileInputStream(Constants.POSTINGS_FREQUENCIES_FILE_PATH + Constants.DAT_FORMAT));
            postingsDocIdStream.skip(docIDsFileOffset);
            postingsFrequenciesStream.skip(frequenciesFileOffset);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeList(){
        try {
            postingsDocIdStream.close();
            postingsFrequenciesStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getVariableByte(InputStream stream){
        ArrayList<Byte> array = new ArrayList<>();
        byte[] nextByte = new byte[1];
        while(true){
            try {
                stream.readNBytes(nextByte, 0, 1);
                array.add(nextByte[0]);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if((nextByte[0] & 0xff) >= 128)
                break;
        }
        int n = array.size();
        byte[] out = new byte[n];
        for (int i = 0; i < n; i++) {
            out[i] = array.get(i);
        }
        return out;
    }

    public void next() throws TerminatedListException {
        if(docIDIncrement >= docIDMaxSize || frequenciesIncrement >= freqMaxSize)
            throw new TerminatedListException();

        byte[] nextDocID = getVariableByte(postingsDocIdStream);
        currentDocID = Utils.decode(nextDocID).get(0);
        docIDIncrement += nextDocID.length;

        byte[] nextFreq = getVariableByte(postingsFrequenciesStream);
        currentFreq = Utils.decode(nextFreq).get(0);
        frequenciesIncrement += nextFreq.length;
    }

    public void nextGEQ(int docid) throws TerminatedListException{
        while(true){
            next();
            if(getDocId() >= docid)
                return;
        }
    }

    public int getDocId(){
        return currentDocID;
    }

    public String getTerm(){
        return term;
    }

    public int getFreq(){
        return currentFreq;
    }
}


