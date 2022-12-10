package it.unipi.exceptions;

public class IllegalQueryType extends Exception {

    public IllegalQueryType() {
        super("Query type not supported...");
    }

    public IllegalQueryType(String type) {
        super("Query of type " + type + " not supported...");
    }
}
