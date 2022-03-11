package utils;

import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;

import java.nio.ByteBuffer;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.HashUtils.bytesToHex;
import static utils.HashUtils.generateNextIdOfId;

@Slf4j
public class PlasmaUtils {

    public static ByteBuffer findEntryWithKey(final PlasmaClient plasmaClient, final String key, final ByteBuffer startBuffer, final int plasmaTimeoutMs) {
        ByteBuffer currentBuffer = startBuffer;
        PlasmaEntry currentEntry = getPlasmaEntryFromBuffer(currentBuffer);

        byte[] nextID = currentEntry.nextPlasmaID;
        while (!key.equals(currentEntry.key) && plasmaClient.contains(nextID)) {
            currentBuffer = plasmaClient.getObjAsByteBuffer(nextID, plasmaTimeoutMs, false);
            currentEntry = getPlasmaEntryFromBuffer(currentBuffer);
            nextID = currentEntry.nextPlasmaID;
        }
        if (key.equals(currentEntry.key)) {
            return currentBuffer;
        } else {
            return null;
        }
    }

    public static byte[] getObjectIdOfNextEntryWithEmptyNextID(final PlasmaClient plasmaClient, final PlasmaEntry startEntry, final byte[] startId, final String keyToCheck, final int plasmaTimeoutMs) throws DuplicateObjectException {
        byte[] currentID = startId;
        byte[] nextID = startEntry.nextPlasmaID;
        while (plasmaClient.contains(nextID)) {
            currentID = nextID;
            final PlasmaEntry nextPlasmaEntry = deserialize(plasmaClient.get(nextID, plasmaTimeoutMs, false));
            log.info(nextPlasmaEntry.key);
            if (nextPlasmaEntry.key.equals(keyToCheck)) {
                throw new DuplicateObjectException(bytesToHex(nextID));
            }
            nextID = nextPlasmaEntry.nextPlasmaID;
        }
        return currentID;
    }

    public static void saveObjectToPlasma(final PlasmaClient plasmaClient, final byte[] id, final byte[] object, final byte[] metadata) throws DuplicateObjectException, PlasmaOutOfMemoryException {
        final ByteBuffer byteBuffer = plasmaClient.create(id, object.length, metadata);
        for (final byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
    }

    public static String findAndDeleteEntryWithKey(final PlasmaClient plasmaClient, final String keyToDelete, final PlasmaEntry startEntry, final byte[] startID, final int plasmaTimeoutMs) {
        final byte[] nextID = startEntry.nextPlasmaID;

        if (keyToDelete.equals(startEntry.key)) {
            log.info("Keys match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                byte[] nextEntryBytes = plasmaClient.get(nextID, plasmaTimeoutMs, false);
                final PlasmaEntry nextEntry = deserialize(nextEntryBytes);
                byte[] nextNextID = nextEntry.nextPlasmaID;
                if (plasmaClient.contains(nextNextID)) {
                    nextEntry.nextPlasmaID = nextID;
                    nextEntryBytes = serialize(nextEntry);
                }
                deleteById(startID, plasmaClient);
                plasmaClient.put(startID, nextEntryBytes, new byte[0]);
                nextEntry.nextPlasmaID = nextNextID;
                return findAndDeleteEntryWithKey(plasmaClient, nextEntry.key, nextEntry, nextID, plasmaTimeoutMs);
            } else {
                log.info("Entry with next id {} does not exist", nextID);
                deleteById(startID, plasmaClient);
                return "204";
            }
        } else {
            log.info("Keys do not match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                final PlasmaEntry nextEntry = deserialize(plasmaClient.get(nextID, plasmaTimeoutMs, false));
                plasmaClient.release(startID);
                return findAndDeleteEntryWithKey(plasmaClient, keyToDelete, nextEntry, nextID, plasmaTimeoutMs);
            } else {
                plasmaClient.release(startID);
                log.info("Entry with next id {} does not exist", nextID);
                return "404";
            }
        }
    }

    public static void saveNewEntryToNextFreeId(final PlasmaClient plasmaClient, final byte[] fullID, final String keyToCheck, final byte[] newPlasmaEntryBytes, final PlasmaEntry plasmaEntry, final int plasmaTimeoutMs) throws DuplicateObjectException {
        final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, fullID, keyToCheck, plasmaTimeoutMs);
        log.info("Next object id with free next id is: {}", objectIdWithFreeNextID);
        final PlasmaEntry plasmaEntryWithEmptyNextID = deserialize(plasmaClient.get(objectIdWithFreeNextID, plasmaTimeoutMs, false));

        final byte[] id = generateNextIdOfId(objectIdWithFreeNextID);

        deleteById(objectIdWithFreeNextID, plasmaClient);

        final PlasmaEntry updatedEntry = new PlasmaEntry(plasmaEntryWithEmptyNextID.key, plasmaEntryWithEmptyNextID.value, id);

        saveObjectToPlasma(plasmaClient, objectIdWithFreeNextID, serialize(updatedEntry), new byte[0]);
        saveObjectToPlasma(plasmaClient, id, newPlasmaEntryBytes, new byte[0]);
    }

    private static void deleteById(final byte[] id, final PlasmaClient plasmaClient) {
        log.info("Deleting {} ...", id);
        while (plasmaClient.contains(id)) {
            plasmaClient.release(id);
            plasmaClient.delete(id);
        }
        log.info("Entry deleted");
    }

    public static PlasmaEntry getPlasmaEntryFromBuffer(final ByteBuffer objectBuffer) {
        final byte[] data = new byte[objectBuffer.remaining()];
        objectBuffer.get(data);
        objectBuffer.position(0);
        return deserialize(data);
    }
}
