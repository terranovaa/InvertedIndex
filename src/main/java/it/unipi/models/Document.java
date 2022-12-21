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

    public int getDocId() {
        return docId;
    }

    public int getLength() {
        return length;
    }

    public String getDocNo() {
        return docNo;
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

    //encode document object as an array of Strings
    public String[] serializeTextual() {
        ArrayList<String> list = new ArrayList<>();
        list.add(docNo);
        list.add(Integer.toString(docId));
        list.add(Integer.toString(length));
        return list.toArray(new String[0]);
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

}
