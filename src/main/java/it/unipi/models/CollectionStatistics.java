package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.Utils;
import jdk.jshell.execution.Util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CollectionStatistics {

    private int numDocs;
    private int numDistinctTerms;
    private int numTotalTerms;

    public CollectionStatistics() {
        numDocs = 0;
        numDistinctTerms = 0;
        numTotalTerms = 0;
    }

    public CollectionStatistics(int numDocs, int numDistinctTerms, int numTotalTerms) {
        this.numDocs = numDocs;
        this.numDistinctTerms = numDistinctTerms;
        this.numTotalTerms = numTotalTerms;
    }

    public int getNumDocs() {
        return numDocs;
    }

    public void setNumDocs(int numDocs) {
        this.numDocs = numDocs;
    }

    public int getNumDistinctTerms() {
        return numDistinctTerms;
    }

    public void setNumDistinctTerms(int numDistinctTerms) {
        this.numDistinctTerms = numDistinctTerms;
    }

    public int getNumTotalTerms() {
        return numTotalTerms;
    }

    public void setNumTotalTerms(int numTotalTerms) {
        this.numTotalTerms = numTotalTerms;
    }

    public void incrementNumDistinctTerms() {
        numDistinctTerms++;
    }

    public void incrementNumTotalTerms() {
        numTotalTerms++;
    }

    /*
    public void writeToDisk() throws FileNotFoundException {

        try (FileOutputStream outputStream = new FileOutputStream(Constants.COLLECTION_STATISTICS_FILE_PATH + Constants.DAT_FORMAT)) {
            outputStream.write(Utils.intToByteArray(numDocs));
            outputStream
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

     */

    public byte[] serializeBinary() {

        byte[] collectionStatistics = new byte[4 * 3];

        System.arraycopy(Utils.intToByteArray(numDocs), 0, collectionStatistics, 0, 4);
        System.arraycopy(Utils.intToByteArray(numDistinctTerms), 0, collectionStatistics, 4, 4);
        System.arraycopy(Utils.intToByteArray(numTotalTerms), 0, collectionStatistics, 8, 4);

        return collectionStatistics;
    }

    public void deserializeBinary(byte[] buffer) {

        numDocs = Utils.byteArrayToInt(buffer, 0);
        numDistinctTerms = Utils.byteArrayToInt(buffer, 4);
        numTotalTerms = Utils.byteArrayToInt(buffer, 8);

    }

    public String serializeToString() {
        return numDocs + "," + numDistinctTerms + "," + numTotalTerms;
    }

    public void deserializeFromString(String buffer) {
        List<String> elements = Arrays.asList(buffer.split(","));
        numDocs = Integer.parseInt(elements.get(0));
        numDistinctTerms = Integer.parseInt(elements.get(1));
        numTotalTerms = Integer.parseInt(elements.get(2));
    }
}
