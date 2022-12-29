package it.unipi.exceptions;

// exception used in case of wrong query type
public class IllegalQueryTypeException extends Exception {
    public IllegalQueryTypeException() {
        super("Query type not supported...");
    }

}
