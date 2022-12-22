package it.unipi.utils;

import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.regex.Pattern;

public class TextProcessingUtils {
    // we use a hashset because contains() is O(1)
    private static final HashSet<String> stopWords;
    // Snowball stemmer, better than Porter
    private static final englishStemmer englishStemmer = new englishStemmer();
    // removes punctuation and strange characters
    static Pattern cleanRegex = Pattern.compile("[^a-zA-Z0-9]");
    // splits by spaces
    static Pattern splitRegex = Pattern.compile(" +");

    static {
        try {
            stopWords = new HashSet<>(Files.readAllLines(Paths.get(Constants.STOPWORDS_PATH)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isAStopWord(String token){
        return stopWords.contains(token);
    }

    // token cannot be longer than MAX_TERM_LEN
    public static String truncateToken(String token) {
        return token.length() > Constants.MAX_TERM_LEN ? token.substring(0, Constants.MAX_TERM_LEN) : token;
    }

    public static String[] tokenize(String document){
        // normalization
        document = document.toLowerCase();
        //remove punctuation and strange characters
        document = cleanRegex.matcher(document).replaceAll(" ");
        //split in tokens
        return splitRegex.split(document);
    }

    public static String stemToken(String token) {
        englishStemmer.setCurrent(token);
        if (englishStemmer.stem()) {
            token = englishStemmer.getCurrent();
        }
        return token;
    }
}
