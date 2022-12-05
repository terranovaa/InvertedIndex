package it.unipi;

import it.unipi.utils.Utils;

import java.nio.charset.StandardCharsets;

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
        System.out.println("docid: " + docId +
                " | docno: " + docNo +
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
     byte[] serialize() {
         // TODO to change too, docno max size is 20 chars ok? 20*6 + 2*4
         final int DOCUMENT_ENTRY_SIZE = 128;

         byte[] documentEntry = new byte[DOCUMENT_ENTRY_SIZE];
         //variable number of bytes
         byte[] docNo = this.getDocNo().getBytes(StandardCharsets.UTF_8);
         //fixed number of bytes, 4 for each integer
         byte[] docId = Utils.intToByteArray(this.getDocId());
         byte[] docLength = Utils.intToByteArray(this.getLength());

         //fill the first part of the buffer with the utf-8 representation of the docno, leave the rest to 0
         System.arraycopy(docNo, 0, documentEntry, 0, docNo.length);
         //fill the last part of the buffer
         System.arraycopy(docId, 0, documentEntry, DOCUMENT_ENTRY_SIZE - 8, 4);
         System.arraycopy(docLength, 0, documentEntry, DOCUMENT_ENTRY_SIZE - 4, 4);
         return documentEntry;
    }


    //decode a disk-based array of bytes representing a document index entry in a Document object
    void deserialize(byte[] buffer) {

        final int DOCUMENT_ENTRY_SIZE = 128;
        //to decode the docNo, detect the position of the first byte equal 0
        int endOfString = 0;

        while(buffer[endOfString] != 0){
            endOfString++;
        }
        //parse only the first part of the buffer until the first byte equal 0
        docNo = new String(buffer, 0, endOfString, StandardCharsets.UTF_8);
        //decode the rest of the buffer
        docId = Utils.byteArrayToInt(buffer, DOCUMENT_ENTRY_SIZE - 8);
        length = Utils.byteArrayToInt(buffer, DOCUMENT_ENTRY_SIZE - 4);
    }
}
