package it.unipi.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Utils {

    public static ArrayList<Byte> encodeNumber(int n) {
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

     public static byte[] encode(List<Integer> numbers) {
        ArrayList<Byte> byteStream = new ArrayList<>();
        List<Byte> bytes;
        for (Integer number: numbers) {
            bytes = encodeNumber(number);
            byteStream.addAll(bytes);
        }
        return byteArrayListToByteArray(byteStream);
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

    public static byte[] byteArrayListToByteArray(ArrayList<Byte> bytes) {
        byte[] bytesPrimitive = new byte[bytes.size()];
        int j = 0;
        for (Byte b: bytes) {
            bytesPrimitive[j++] = b;
        }
        return bytesPrimitive;
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
