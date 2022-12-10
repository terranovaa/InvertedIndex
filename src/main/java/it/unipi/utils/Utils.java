package it.unipi.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.tartarus.snowball.ext.englishStemmer;

import static java.lang.Math.log;

public final class Utils {
    private static final HashSet<String> stopWords;
    private static final englishStemmer englishStemmer = new englishStemmer();

    static {
        try {
            stopWords = new HashSet<>(Files.readAllLines(Paths.get(Constants.STOPWORDS_PATH)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String stemming(String token){
        englishStemmer.setCurrent(token);
        if (englishStemmer.stem()) {
            token = englishStemmer.getCurrent();
        }
        return token;
    }

    public static void setupEnvironment(){
        try {
            for(String directory: Constants.DIRECTORIES_PATHS)
                Files.createDirectories(Paths.get(directory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO: Add logic, malformed lines?
    public static boolean validDocument(String document){
        // check empty page
        if(document.length()==0)
            return false;
        return true;
    }

    public static boolean validToken(String token){
        //stop word removal & stemming
        if(stopWords.contains(token)) //if the token is a stop word don't consider it
            return false;

        if (token.length() > Constants.MAX_TERM_LEN)
            return false;
        return true;
    }

    public static String[] tokenize(String document){
        // normalization
        document = document.toLowerCase();
        // TODO: more complex tokenization? Like BPE?
        //remove punctuation and strange characters
        document = document.replaceAll("[^a-z0-9\\s]", " ");
        //split in tokens
        return document.split(" ");
    }


    public static ArrayList<Byte> encodeNumberToArrayList(int n) {
        int number = n;
        ArrayList<Byte> bytes = new ArrayList<>();
        while (true) {
            bytes.add(0, (byte) (number % 128));
            if (number < 128) break;
            number = Math.floorDiv(number, 128);
        }
        bytes.set(bytes.size() - 1, (byte) (bytes.get(bytes.size() - 1) | 0x80));
        return bytes;
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

    //given an integer return the byte representation
    public static byte[] intToByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static int byteArrayToInt(byte[] value, int startIndex) {
        //return ByteBuffer.wrap(value).getInt(startIndex);
        return ((value[startIndex] & 0xFF) << 24) |
                ((value[startIndex + 1] & 0xFF) << 16) |
                ((value[startIndex + 2] & 0xFF) << 8 ) |
                ((value[startIndex + 3] & 0xFF));
    }
}
