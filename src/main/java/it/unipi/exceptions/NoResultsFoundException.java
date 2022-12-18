package it.unipi.exceptions;

public class NoResultsFoundException extends Exception{
    public NoResultsFoundException() {
        super("There are no documents containing all the specified terms");
    }
}
