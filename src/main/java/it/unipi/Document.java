package it.unipi;

import java.util.ArrayList;

public class Document {
    private int docid;
    private int length; // number of characters
    private String docno;

    public Document() {
    }

    public Document(int docid, String docno, int length) {
        this.docid = docid;
        this.docno = docno;
        this.length = length;
    }

    public void printInfo(){
        System.out.println("docid: " + docid +
                " | docno: " + docno +
                " | length: " + length);
        System.out.println("\n------------------------------");
    }

    public int getDocid() {
        return docid;
    }

    public int getLength() {
        return length;
    }

    public String getDocno() {
        return docno;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public void setDocno(String docno) {
        this.docno = docno;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
