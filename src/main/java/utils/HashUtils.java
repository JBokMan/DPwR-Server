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

    public static byte[] generateID(String key) {
        // Generate plasma object id
        byte[] id = new byte[0];
        try {
            id = getMD5Hash(key);
        } catch (NoSuchAlgorithmException e) {
            log.error("The MD5 hash algorithm was not found.", e);
        }
        final byte[] fullID = ArrayUtils.addAll(id, new byte[4]);
        log.info("FullID: {} of key: {}", fullID, key);
        return fullID;
    }

    public static byte[] getMD5Hash(final String text) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        byte[] id = messageDigest.digest(text.getBytes(StandardCharsets.UTF_8));
        if (TEST_MODE) {
            if (text.contains("hash_collision_test")) {
                id = new byte[16];
            }
            if (text.contains("timeout_test")) {
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
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

    public static byte[] generateNextIdOfId(byte[] objectIdWithFreeNextID) {
        String idAsHexString = bytesToHex(objectIdWithFreeNextID);
        String tailEnd = idAsHexString.substring(idAsHexString.length() - 4);
        int tailEndInt = Integer.parseInt(tailEnd);
        tailEndInt += 1;
        String newID = idAsHexString.substring(0, idAsHexString.length() - 4) + String.format("%04d", tailEndInt);
        return HexFormat.of().parseHex(newID);
    }

    public static String bytesToHex(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
