package it.unipi.utils;

import org.tartarus.snowball.ext.englishStemmer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.regex.Pattern;

public class TextProcessingUtils {
    private static final HashSet<String> stopWords;
    private static final englishStemmer englishStemmer = new englishStemmer();
    static Pattern cleanRegex = Pattern.compile("[^a-zA-Z0-9]");
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
