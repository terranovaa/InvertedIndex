package it.unipi.exceptions;

public class TermNotFoundException extends Exception {

    public TermNotFoundException() {
        super("The query term could not be found in the lexicon");
    }
}
