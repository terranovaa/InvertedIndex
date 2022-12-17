package it.unipi.models;

import it.unipi.utils.Utils;

import java.util.Arrays;
import java.util.List;

public class CollectionStatistics {

    private int numDocs;
    private double avgDocLen;

    public CollectionStatistics() {
        numDocs = 0;
        avgDocLen = 0;
    }

    public int getNumDocs() {
        return numDocs;
    }

    public void setNumDocs(int numDocs) {
        this.numDocs = numDocs;
    }

    public double getAvgDocLen() {
        return avgDocLen;
    }

    public void setAvgDocLen(double avgDocLen) {
        this.avgDocLen = avgDocLen;
    }

    public byte[] serializeBinary() {

        byte[] collectionStatistics = new byte[4 * 3];

        System.arraycopy(Utils.intToByteArray(numDocs), 0, collectionStatistics, 0, 4);
        System.arraycopy(Utils.doubleToByteArray(avgDocLen), 0, collectionStatistics, 4, 8);

        return collectionStatistics;
    }

    public void deserializeBinary(byte[] buffer) {

        numDocs = Utils.byteArrayToInt(buffer, 0);
        avgDocLen = Utils.byteArrayToDouble(buffer, 4);
    }

    public String serializeToString() {
        return numDocs + "," + avgDocLen;
    }

    public void deserializeFromString(String buffer) {
        List<String> elements = Arrays.asList(buffer.split(","));
        numDocs = Integer.parseInt(elements.get(0));
        avgDocLen = Double.parseDouble(elements.get(1));
    }
}
