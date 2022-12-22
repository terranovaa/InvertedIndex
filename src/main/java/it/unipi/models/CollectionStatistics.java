package it.unipi.models;

import it.unipi.utils.EncodingUtils;

public class CollectionStatistics {

    // number of documents in the collection
    private int numDocs;
    // average document length
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

        System.arraycopy(EncodingUtils.intToByteArray(numDocs), 0, collectionStatistics, 0, 4);
        System.arraycopy(EncodingUtils.doubleToByteArray(avgDocLen), 0, collectionStatistics, 4, 8);

        return collectionStatistics;
    }

    public void deserializeBinary(byte[] buffer) {
        numDocs = EncodingUtils.byteArrayToInt(buffer, 0);
        avgDocLen = EncodingUtils.byteArrayToDouble(buffer, 4);
    }

    public String serializeToString() {
        return numDocs + "," + avgDocLen;
    }

}
