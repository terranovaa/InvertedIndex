package it.unipi.models;

import it.unipi.utils.Constants;
import it.unipi.utils.EncodingUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Document {
    private int docId;
    private int length; // number of characters
    private String docNo;

    public Document() {
    }

    public Document(int docId, String docNo, int length) {
        this.docId = docId;
        this.docNo = docNo;
        this.length = length;
    }

    public void printInfo(){
        System.out.println("doc_id: " + docId +
                " | doc_no: " + docNo +
                " | length: " + length);
        System.out.println("\n------------------------------");
    }

    public int getDocId() {
        return docId;
    }

    public int getLength() {
        return length;
    }

    public String getDocNo() {
        return docNo;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public void setLength(int length) {
        this.length = length;
    }


    //encode document object as an array of bytes with fixed dimension
     public byte[] serializeBinary() {

         byte[] documentEntry = new byte[Constants.DOCUMENT_ENTRY_SIZE];
         //variable number of bytes
         byte[] docNo = this.getDocNo().getBytes(StandardCharsets.UTF_8);
         //fixed number of bytes, 4 for each integer
         byte[] docId = EncodingUtils.intToByteArray(this.getDocId());
         byte[] docLength = EncodingUtils.intToByteArray(this.getLength());

         //fill the first part of the buffer with the utf-8 representation of the doc_no, leave the rest to 0
         System.arraycopy(docNo, 0, documentEntry, 0, docNo.length);
         //fill the last part of the buffer
         System.arraycopy(docId, 0, documentEntry, Constants.DOCUMENT_ENTRY_SIZE - 8, 4);
         System.arraycopy(docLength, 0, documentEntry, Constants.DOCUMENT_ENTRY_SIZE - 4, 4);
         return documentEntry;
    }

    public byte[][] serializeBinarySplit() {
        byte[][] output = new byte[2][];
        //variable number of bytes

        byte[] documentEntry = new byte[Constants.DOCUMENT_ENTRY_SIZE_SPLIT1];
        //fixed number of bytes, 4 for each integer
        byte[] docId = EncodingUtils.intToByteArray(this.getDocId());
        byte[] docLength = EncodingUtils.intToByteArray(this.getLength());

        //fill the last part of the buffer
        System.arraycopy(docId, 0, documentEntry, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 - 8, 4);
        System.arraycopy(docLength, 0, documentEntry, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 - 4, 4);
        output[0] = documentEntry;

        byte[] documentEntry2 = new byte[Constants.DOCUMENT_ENTRY_SIZE_SPLIT2];

        //variable number of bytes
        byte[] docNo = this.getDocNo().getBytes(StandardCharsets.UTF_8);

        //fill the last part of the buffer
        System.arraycopy(docNo, 0, documentEntry2, 0, docNo.length);
        System.arraycopy(docId, 0, documentEntry2, Constants.DOCUMENT_ENTRY_SIZE_SPLIT2 - 4, 4);
        output[1] = documentEntry2;
        return output;
    }

    //encode document object as an array of Strings
    public String[] serializeTextual() {
        ArrayList<String> list = new ArrayList<>();
        list.add(docNo);
        list.add(Integer.toString(docId));
        list.add(Integer.toString(length));
        return list.toArray(new String[0]);
    }

    public String[][] serializeTextualSplit() {
        ArrayList<String> list = new ArrayList<>();
        list.add(Integer.toString(docId));
        list.add(Integer.toString(length));
        ArrayList<String> list2 = new ArrayList<>();
        list2.add(Integer.toString(docId));
        list2.add(docNo);
        return new String[][]{list.toArray(new String[0]), list2.toArray(new String[0])};
    }


    //decode a disk-based array of bytes representing a document index entry in a Document object
    public void deserializeBinary(byte[] buffer) {
        //to decode the docNo, detect the position of the first byte equal 0
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        docNo = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        docId = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE - 8);
        length = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE - 4);
    }

    public void deserializeBinarySplit(byte[][] input) {
        //to decode the docNo, detect the position of the first byte equal 0
        byte[] buffer = input[0];
        int endOfString = 0;
        while(buffer[endOfString] != 0){
            endOfString++;
        }

        //decode the rest of the buffer
        docId = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE - 8);
        length = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE - 4);

        buffer = input[1];
        //parse only the first part of the buffer until the first byte equal 0
        docNo = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
    }

    public void deserializeBinarySplit1(byte[] buffer){
        docId = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 - 8);
        length = EncodingUtils.byteArrayToInt(buffer, Constants.DOCUMENT_ENTRY_SIZE_SPLIT1 - 4);
    }
}
