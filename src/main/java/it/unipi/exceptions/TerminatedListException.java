package it.unipi.exceptions;

public class TerminatedListException extends Exception {

    public TerminatedListException() {
        super("Posting list terminated...");
    }

}
