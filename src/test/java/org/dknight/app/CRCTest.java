package org.dknight.app;

import org.junit.Test;

import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Created by fanming.chen on 2016/9/5 0005.
 */
public class CRCTest {

    @Test
    public void CRCCalculateTest() {
        String input = "Java Code Geeks - Java Examples";

        String lenInput = input.length() + input;

        // get bytes from string
        byte bytes[] = lenInput.getBytes();

        Checksum checksum = new CRC32();

        // update the current checksum with the specified array of bytes
        checksum.update(bytes, 0, bytes.length);

        // get the current checksum value
        long checksumValue = checksum.getValue();

        System.out.println("CRC32 checksum for input string is: " + checksumValue);

        checksum.reset();

        checksum.update(input.length());

        byte[] inBytes = input.getBytes();
        checksum.update(inBytes, 0, inBytes.length);

        long sepChecksum = checksum.getValue();
    }

    @Test
    public void integerBytesTest() {
        int a = 1;
        int b = (0x1<<8) + 1;

        Checksum checksum = new CRC32();
        checksum.reset();
        checksum.update(a);
        long aSum = checksum.getValue();

        checksum.reset();
        checksum.update(b);
        long bSum = checksum.getValue();

        checksum.reset();
        checksum.update(ByteUtils.toBytes(b), 0, 4);
        long bbSum = checksum.getValue();
    }
}
