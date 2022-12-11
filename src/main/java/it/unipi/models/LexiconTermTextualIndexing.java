package it.unipi.models;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LexiconTermTextualIndexing extends LexiconTermIndexing {

    public LexiconTermTextualIndexing() {
    }

    public LexiconTermTextualIndexing(String term) {
        super(term);
    }

    public String[] serialize() {
        ArrayList<String> list = new ArrayList<>();
        list.add(term);
        list.add(Integer.toString(documentFrequency));
        list.add(Integer.toString(collectionFrequency));
        return list.toArray(new String[0]);
    }

    public void deserialize(String buffer) {
        List<String> elements = Arrays.asList(buffer.split(","));
        term = elements.get(0);
        documentFrequency = Integer.parseInt(elements.get(1));
        collectionFrequency = Integer.parseInt(elements.get(2));
    }

    public void writeToDisk(BufferedWriter docIDStream, BufferedWriter frequenciesStream, BufferedWriter lexiconStream) throws IOException {
        //docIDs
        List<Integer> docIDs = this.getPostingListDocIds();
        for(int i = 0; i < docIDs.size(); ++i)
            if(i != docIDs.size()-1)
                docIDStream.write(docIDs.get(i).toString()+",");
            else docIDStream.write(docIDs.get(i).toString()+"\n");

        // frequencies
        List<Integer> frequencies = this.getPostingListFrequencies();
        for(int i = 0; i < frequencies.size(); ++i)
            if(i != frequencies.size()-1)
                frequenciesStream.write(frequencies.get(i).toString()+",");
            else frequenciesStream.write(frequencies.get(i).toString()+"\n");

        //lexicon term
        String[] lexiconEntry = this.serialize();
        for(int i = 0; i < lexiconEntry.length; ++i)
            if(i != lexiconEntry.length-1)
                lexiconStream.write(lexiconEntry[i]+",");
            else lexiconStream.write(lexiconEntry[i]+"\n");
    }
}
