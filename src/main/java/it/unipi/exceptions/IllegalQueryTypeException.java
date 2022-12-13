package it.unipi.exceptions;

public class IllegalQueryTypeException extends Exception {

    public IllegalQueryTypeException() {
        super("Query type not supported...");
    }

    public IllegalQueryTypeException(String type) {
        super("Query of type " + type + " not supported...");
    }
}
