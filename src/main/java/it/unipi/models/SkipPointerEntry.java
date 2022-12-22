package it.unipi.models;

// immutable class used to store the skip blocks offsets
public record SkipPointerEntry(long docIdOffset, long freqOffset) {

}
