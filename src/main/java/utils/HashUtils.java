package utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HashUtils {
    private static final boolean TEST_MODE = true;

    public static byte[] generateID(final String key) {
        // Generate plasma object id
        byte[] id = new byte[0];
        try {
            id = getMD5Hash(key);
        } catch (final NoSuchAlgorithmException e) {
            log.error("The MD5 hash algorithm was not found.", e);
        }
        final byte[] fullID = ArrayUtils.addAll(id, new byte[4]);
        log.info("FullID: {} of key: {}", fullID, key);
        return fullID;
    }

    private static byte[] getMD5Hash(final String text) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        byte[] id = messageDigest.digest(text.getBytes(StandardCharsets.UTF_8));
        if (TEST_MODE) {
            if (text.contains("hash_collision_test")) {
                id = new byte[16];
            }
            if (text.contains("timeout_test")) {
                try {
                    TimeUnit.MILLISECONDS.sleep(5000);
                } catch (final InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        }
        // If all bits are zero there are problems with the next entry id's of the plasma entry's
        if (Arrays.equals(id, new byte[16])) {
            // Set the first bit to 1
            id[0] |= 1 << (0);
        }
        return id;
    }

    public static byte[] generateNextIdOfId(final byte[] id) {
        final String idAsHexString = bytesToHex(id);
        final String tailEnd = idAsHexString.substring(idAsHexString.length() - 4);
        int tailEndInt = Integer.parseInt(tailEnd);
        tailEndInt += 1;
        final String newID = idAsHexString.substring(0, idAsHexString.length() - 4) + String.format("%04d", tailEndInt);
        return HexFormat.of().parseHex(newID);
    }

    private static String bytesToHex(final byte[] raw) {
        final StringBuilder buffer = new StringBuilder();
        for (final byte b : raw) {
            buffer.append(Character.forDigit((b >> 4) & 0xF, 16));
            buffer.append(Character.forDigit((b & 0xF), 16));
        }
        return buffer.toString();
    }
}
