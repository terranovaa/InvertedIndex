package it.unipi.exceptions;

public class IllegalQueryTypeException extends Exception {

    public IllegalQueryTypeException() {
        super("Query type not supported...");
    }

}
