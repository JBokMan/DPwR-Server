package utils;

import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;

import java.nio.ByteBuffer;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.HashUtils.generateNextIdOfId;

@Slf4j
public class PlasmaUtils {

    public static PlasmaEntry findEntryWithKey(PlasmaClient plasmaClient, String key, PlasmaEntry startEntry) {
        PlasmaEntry currentEntry = startEntry;
        byte[] nextID = currentEntry.nextPlasmaID;
        while (!key.equals(currentEntry.key) && plasmaClient.contains(nextID)) {
            currentEntry = deserialize(plasmaClient.get(nextID, 100, false));
            nextID = currentEntry.nextPlasmaID;
        }
        if (key.equals(currentEntry.key)) {
            return currentEntry;
        } else {
            return null;
        }
    }

    public static byte[] getObjectIdOfNextEntryWithEmptyNextID(PlasmaClient plasmaClient, final PlasmaEntry startEntry, byte[] startId) {
        //TODO throw error when keyToPut already exists in another entry
        byte[] currentID = startId;
        byte[] nextID = startEntry.nextPlasmaID;
        while (plasmaClient.contains(nextID)) {
            currentID = nextID;
            final PlasmaEntry nextPlasmaEntry = deserialize(plasmaClient.get(nextID, 100, false));
            nextID = nextPlasmaEntry.nextPlasmaID;
        }
        return currentID;
    }

    public static void saveObjectToPlasma(PlasmaClient plasmaClient, byte[] id, byte[] object, byte[] metadata) throws DuplicateObjectException, PlasmaOutOfMemoryException {
        ByteBuffer byteBuffer = plasmaClient.create(id, object.length, metadata);
        for (byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
    }

    public static String findAndDeleteEntryWithKey(PlasmaClient plasmaClient, String keyToDelete, PlasmaEntry startEntry, byte[] startID) {
        byte[] nextID = startEntry.nextPlasmaID;

        if (keyToDelete.equals(startEntry.key)) {
            log.info("Keys match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                byte[] nextEntryBytes = plasmaClient.get(nextID, 100, false);
                PlasmaEntry nextEntry = deserialize(nextEntryBytes);
                byte[] nextNextID = nextEntry.nextPlasmaID;
                if (plasmaClient.contains(nextNextID)) {
                    nextEntry.nextPlasmaID = nextID;
                    nextEntryBytes = serialize(nextEntry);
                }
                deleteById(startID, plasmaClient);
                plasmaClient.put(startID, nextEntryBytes, new byte[0]);
                nextEntry.nextPlasmaID = nextNextID;
                return findAndDeleteEntryWithKey(plasmaClient, nextEntry.key, nextEntry, nextID);
            } else {
                log.info("Entry with next id {} does not exist", nextID);
                deleteById(startID, plasmaClient);
                return "204";
            }
        } else {
            log.info("Keys do not match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                PlasmaEntry nextEntry = deserialize(plasmaClient.get(nextID, 100, false));
                return findAndDeleteEntryWithKey(plasmaClient, keyToDelete, nextEntry, nextID);
            } else {
                log.info("Entry with next id {} does not exist", nextID);
                return "404";
            }
        }
    }

    public static void saveNewEntryToNextFreeId(PlasmaClient plasmaClient, byte[] fullID, byte[] newPlasmaEntryBytes, PlasmaEntry plasmaEntry) {
        byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, fullID);
        log.info("Next object id with free next id is: {}", objectIdWithFreeNextID);
        PlasmaEntry plasmaEntryWithEmptyNextID = deserialize(plasmaClient.get(objectIdWithFreeNextID, 100, false));

        byte[] id = generateNextIdOfId(objectIdWithFreeNextID);

        deleteById(objectIdWithFreeNextID, plasmaClient);

        PlasmaEntry updatedEntry = new PlasmaEntry(plasmaEntryWithEmptyNextID.key, plasmaEntryWithEmptyNextID.value, id);

        saveObjectToPlasma(plasmaClient, objectIdWithFreeNextID, serialize(updatedEntry), new byte[0]);
        saveObjectToPlasma(plasmaClient, id, newPlasmaEntryBytes, new byte[0]);
    }

    private static void deleteById(byte[] id, PlasmaClient plasmaClient) {
        log.info("Deleting {} ...", id);
        while (plasmaClient.contains(id)) {
            plasmaClient.release(id);
            plasmaClient.delete(id);
        }
        log.info("Entry deleted");
    }
}
