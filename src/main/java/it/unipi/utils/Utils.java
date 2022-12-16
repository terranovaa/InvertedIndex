package it.unipi.utils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.tartarus.snowball.ext.englishStemmer;

import static java.lang.Math.log;

// TODO split utils (ex. EncodingUtils, TextProcessingUtils...)
public final class Utils {
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

    public static void setupEnvironment(){
        try {
            for(String directory: Constants.DIRECTORIES_PATHS)
                Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteTemporaryFolders(){

        for(String directory: Constants.TEMPORARY_DIRECTORIES_PATHS) {
            Path pathToBeDeleted = Paths.get(directory);
            try (Stream<Path> files = Files.walk(pathToBeDeleted)) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        //document = document.replaceAll("[^a-z0-9\\s]", " ");
        // removing control characters
        document = cleanRegex.matcher(document).replaceAll(" ");
        //split in tokens
        return splitRegex.split(document);
    }

    public static String stemToken(String token){
        englishStemmer.setCurrent(token);
        if (englishStemmer.stem()) {
            token = englishStemmer.getCurrent();
        }
        return token;
    }

    public static byte[] encodeNumber(int n) {
        if (n == 0) {
            return new byte[]{(byte) 128};
        }
        int i = (int) (log(n) / log(128)) + 1;
        byte[] bytes = new byte[i];
        int j = i - 1;
        do {
            bytes[j--] = (byte) (n % 128);
            n /= 128;
        } while (j >= 0);
        bytes[i - 1] += 128;
        return bytes;
    }

    public static byte[] encode(List<Integer> numbers) {
        byte[] byteStream;
        try {
            byteStream = new byte[getEncodingLength(numbers)];
        } catch (NegativeArraySizeException e) {
            byteStream = new byte[50];
            e.printStackTrace();
        }
        byte[] bytes;
        int i = 0;
        for (Integer number: numbers) {
            bytes = encodeNumber(number);
            for (byte byteElem: bytes)
                byteStream[i++] = byteElem;
        }
        return byteStream;
    }

    public static int getEncodingLength(List<Integer> numbers){
        int bytesLength = 0;
        for (Integer number: numbers) {
            if (number == 0) {
                bytesLength += 1;
            } else {
                bytesLength += (int) (log(number) / log(128)) + 1;
            }
        }
        return bytesLength;
    }

    // TODO merge the following two functions
    public static ArrayList<Integer> decode(byte[] byteStream) {
        ArrayList<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (byte byteElem : byteStream) {
            int unsignedByte = byteElem & 0xff;
            if (unsignedByte < 128) {
                n = 128 * n + unsignedByte;
            } else {
                n = 128 * n + (unsignedByte - 128);
                numbers.add(n);
                n = 0;
            }
        }
        return numbers;
    }

    public static ArrayList<Integer> decode(List<Byte> byteStream) {
        ArrayList<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (byte byteElem : byteStream) {
            int unsignedByte = byteElem & 0xff;
            if (unsignedByte < 128) {
                n = 128 * n + unsignedByte;
            } else {
                n = 128 * n + (unsignedByte - 128);
                numbers.add(n);
                n = 0;
            }
        }
        return numbers;
    }

    //given an integer return the byte representation
    public static byte[] intToByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static byte[] longToByteArray(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    public static byte[] intListToByteArray(List<Integer> values) {
        byte[] bytes = new byte[values.size() * 4];
        int i = 0;
        for (Integer value: values) {
            byte[] tempBytes = intToByteArray(value);
            System.arraycopy(tempBytes, 0, bytes, (i * 4), 4);
            i++;
        }
        return bytes;
    }

    public static int byteArrayToInt(byte[] value, int startIndex) {
        return ByteBuffer.wrap(value).getInt(startIndex);
    }

    public static long byteArrayToLong(byte[] value, int startIndex) {
        return ByteBuffer.wrap(value).getLong(startIndex);
    }
}
