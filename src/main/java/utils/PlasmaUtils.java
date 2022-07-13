package utils;

import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

@Slf4j
public class PlasmaUtils {
    public static void deleteById(final PlasmaClient plasmaClient, final byte[] id) {
        log.info("Deleting {} ...", id);
        while (plasmaClient.contains(id)) {
            plasmaClient.release(id);
            plasmaClient.delete(id);
        }
        log.info("Entry deleted");
    }

    public static void saveObjectToPlasma(final PlasmaClient plasmaClient, final byte[] id, final byte[] object) throws DuplicateObjectException, PlasmaOutOfMemoryException {
        log.info("Save object with id: {} to plasma", id);
        final ByteBuffer byteBuffer = plasmaClient.create(id, object.length, new byte[0]);
        for (final byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
    }

    public static PlasmaEntry getPlasmaEntry(final PlasmaClient client, final byte[] id, final int timeoutMs) {
        final byte[] entry = client.get(id, timeoutMs, false);
        return deserialize(entry);
    }

    public static void updateNextIdOfEntry(final PlasmaClient plasmaClient, final byte[] idToUpdate, final byte[] newNextId, final int plasmaTimeoutMs) {
        log.info("Update entry of id {}", idToUpdate);
        final PlasmaEntry entryToUpdate = getPlasmaEntry(plasmaClient, idToUpdate, plasmaTimeoutMs);
        deleteById(plasmaClient, idToUpdate);
        final PlasmaEntry updatedEntry = new PlasmaEntry(entryToUpdate.key, entryToUpdate.value, newNextId);
        saveObjectToPlasma(plasmaClient, idToUpdate, serialize(updatedEntry));
    }

    public static byte[] getObjectIdOfNextEntryWithEmptyNextID(final PlasmaClient plasmaClient, final PlasmaEntry startEntry, final byte[] startId, final String keyToCheck, final int plasmaTimeoutMs) {
        if (startEntry.key.equals(keyToCheck)) {
            return new byte[0];
        }
        byte[] currentID = startId;
        byte[] nextID = startEntry.nextPlasmaID;
        while (plasmaClient.contains(nextID)) {
            currentID = nextID;
            final PlasmaEntry nextPlasmaEntry = deserialize(plasmaClient.get(nextID, plasmaTimeoutMs, false));
            if (nextPlasmaEntry.key.equals(keyToCheck)) {
                return new byte[0];
            }
            nextID = nextPlasmaEntry.nextPlasmaID;
        }
        log.info("Next object id with free next id is: {}", currentID);
        return currentID;
    }

    public static PlasmaEntry getPlasmaEntryFromBuffer(final ByteBuffer objectBuffer) throws IOException, ClassNotFoundException {
        final byte[] data = new byte[objectBuffer.remaining()];
        objectBuffer.get(data);
        objectBuffer.position(0);
        return deserializePlasmaEntry(data);
    }

    public static PlasmaEntry deserializePlasmaEntry(final byte[] entryBytes) throws IOException, ClassNotFoundException {
        final PlasmaEntry entry;
        try (final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(entryBytes)) {
            try (final ObjectInput objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                entry = (PlasmaEntry) objectInputStream.readObject();
            }
        }
        return entry;
    }

    public static byte[] serializePlasmaEntry(final PlasmaEntry entry) throws IOException {
        final byte[] entryBytes;
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (final ObjectOutput objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(entry);
                entryBytes = byteArrayOutputStream.toByteArray();
            }
        }
        return entryBytes;
    }

    public static ByteBuffer findEntryWithKey(final PlasmaClient plasmaClient, final String key, final ByteBuffer startBuffer, final int plasmaTimeoutMs) throws IOException, ClassNotFoundException {
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

    public static String findAndDeleteEntryWithKey(final PlasmaClient plasmaClient, final String keyToDelete, final PlasmaEntry startEntry, final byte[] startID, final byte[] previousID, final int plasmaTimeoutMs) {
        final byte[] nextID = startEntry.nextPlasmaID;

        if (keyToDelete.equals(startEntry.key)) {
            log.info("Keys match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                byte[] nextEntryBytes = plasmaClient.get(nextID, plasmaTimeoutMs, false);
                final PlasmaEntry nextEntry = deserialize(nextEntryBytes);
                final byte[] nextNextID = nextEntry.nextPlasmaID;
                if (plasmaClient.contains(nextNextID)) {
                    nextEntry.nextPlasmaID = nextID;
                    nextEntryBytes = serialize(nextEntry);
                }
                deleteById(plasmaClient, startID);
                plasmaClient.put(startID, nextEntryBytes, new byte[0]);
                nextEntry.nextPlasmaID = nextNextID;
                return findAndDeleteEntryWithKey(plasmaClient, nextEntry.key, nextEntry, nextID, startID, plasmaTimeoutMs);
            } else {
                log.info("Entry with next id {} does not exist", nextID);
                deleteById(plasmaClient, startID);
                log.info("Previous id : {}", previousID);
                if (!Arrays.equals(previousID, new byte[20])) {
                    updateNextIdOfEntry(plasmaClient, previousID, new byte[20], plasmaTimeoutMs);
                }
                return "221";
            }
        } else {
            log.info("Keys do not match");
            if (plasmaClient.contains(nextID)) {
                log.info("Entry with next id {} exists", nextID);
                final PlasmaEntry nextEntry = deserialize(plasmaClient.get(nextID, plasmaTimeoutMs, false));
                return findAndDeleteEntryWithKey(plasmaClient, keyToDelete, nextEntry, nextID, startID, plasmaTimeoutMs);
            } else {
                log.info("Entry with next id {} does not exist", nextID);
                return "421";
            }
        }
    }
}
