package utils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class HashUtilsTest {

    @Test
    void generateIDReturnsATwentyBytesLongId() {
        final String key = "This is a key";

        final byte[] id = HashUtils.generateID(key);

        assertEquals(20, id.length);
    }

    @Test
    void generateIDReturnsTheSameIDForTheSameKeyEveryTime() {
        final String key = "This is a key";

        final byte[] id = HashUtils.generateID(key);
        final byte[] id1 = HashUtils.generateID(key);

        assertArrayEquals(id, id1);
    }

    @Test
    void generateIDReturnsGenerallyTwoDifferentIDsForDifferentKeys() {
        final String key = "This is a key";
        final String key1 = "This is a key1";

        final byte[] id = HashUtils.generateID(key);
        final byte[] id1 = HashUtils.generateID(key1);

        assertFalse(Arrays.equals(id,id1));
    }

    @Test
    void generateNextIdOfIdSetsLastByteToOneIfItsZero() {
        final byte[] id = new byte[20];

        final byte[] nextId = HashUtils.generateNextIdOfId(id);

        assertEquals(1, nextId[19]);
    }

    @Test
    void generateNextIdOfIdSetsLastByteToTwoIfItsOne() {
        final byte[] id = new byte[20];
        id[19] = 1;

        final byte[] nextId = HashUtils.generateNextIdOfId(id);

        assertEquals(2, nextId[19]);
    }
}