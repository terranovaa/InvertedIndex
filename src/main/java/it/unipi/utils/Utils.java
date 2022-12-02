package it.unipi.utils;

import java.util.ArrayList;
import java.util.List;

public final class Utils {

    public static ArrayList<Byte> encodeNumber(int n) {
        int number = n;
        ArrayList<Byte> bytes = new ArrayList<>();
        while (true) {
            bytes.add(0, (byte) (number % 128));
            if (number < 128) break;
            number = Math.floorDiv(n, 128);
        }
        bytes.set(bytes.size() - 1, (byte) (bytes.get(bytes.size() - 1) | 0x80));
        return bytes;
    }

     public static ArrayList<Byte> encode(List<Integer> numbers) {
        ArrayList<Byte> byteStream = new ArrayList<>();
        ArrayList<Byte> bytes;
        for (Integer number: numbers) {
            bytes = encodeNumber(number);
            byteStream.addAll(bytes);
        }
        return byteStream;
    }

    public static ArrayList<Integer> decode(List<Byte> byteStream) {
        ArrayList<Integer> numbers = new ArrayList<>();
        int n = 0;
        for (Byte byteElem : byteStream) {
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
}
