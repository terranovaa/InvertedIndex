package it.unipi.utils;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public ArrayList<Integer> encodeNumber(int n) {
        int number = n;
        ArrayList<Integer> bytes = new ArrayList<>();
        while (true) {
            bytes.add(0, number % 128);
            if (n < 128) break;
            number = Math.floorDiv(n, 128);
        }
        bytes.set(bytes.size() - 1, bytes.get(bytes.size() - 1) + 128);
        return bytes;
    }

    public ArrayList<Integer> encode(List<Integer> numbers) {
        ArrayList<Integer> byteStream = new ArrayList<>();
        ArrayList<Integer> bytes = new ArrayList<>();
        for (Integer number: numbers) {
            bytes = encodeNumber(number);
            byteStream.addAll(bytes);
        }
        return byteStream;
    }

    public ArrayList<Integer> decode(List<Integer> byteStream) {
        ArrayList<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (Integer integer : byteStream) {
            if (integer < 128) {
                n = 128 * n + integer;
            } else {
                n = 128 * n + (integer - 128);
                numbers.add(n);
                n = 0;
            }
        }
        return numbers;
    }
}
