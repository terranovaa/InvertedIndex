package it.unipi.exceptions;

// exception used in case there are 0 documents containing the terms specified
public class NoResultsFoundException extends Exception{
    public NoResultsFoundException() {
        super("There are no documents containing all the specified terms");
    }
}
