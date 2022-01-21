package server;

import event.listener.OnMessageEventListenerImpl;
import infinileap.server.InfinileapServer;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class InfinimumDBServer {

    final int serverID = 0;
    int serverCount = 1;

    private PlasmaClient plasmaClient;
    private final String plasmaFilePath;

    private final InfinileapServer infinileapServer;

    public InfinimumDBServer(String plasmaFilePath, String listenAddress, Integer listenPort) {
        this.plasmaFilePath = plasmaFilePath;
        this.infinileapServer = new InfinileapServer(listenAddress, listenPort);
        this.infinileapServer.registerOnMessageEventListener(new OnMessageEventListenerImpl());
        connectPlasma();
        listen();
    }

    /*public InfinimumDBServer(String plasmaFilePath, String listenAddress, Integer listeningPort,
                             String mainServerHostAddress, Integer mainServerPort) {
        this.plasmaFilePath = plasmaFilePath;
        this.infinileapServer = new InfinileapServer(listenAddress);
        this.infinileapServer.registerOnMessageEventListener(this);
        connectPlasma();
    }*/

    private void connectPlasma() {
        System.loadLibrary("plasma_java");
        try {
            this.plasmaClient = new PlasmaClient(plasmaFilePath, "", 0);
        } catch (Exception e) {
            System.err.println("PlasmaDB could not be reached");
        }
    }

    public void put(byte[] id, byte[] object) {
        try {
            this.plasmaClient.put(id, object, new byte[0]);
        } catch (DuplicateObjectException e) {
            this.plasmaClient.delete(id);
            this.put(id, object);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public byte[] get(byte[] uuid) {
        try {
            return this.plasmaClient.get(uuid, 100, false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return new byte[0];
    }

    public byte[] generateUUID(byte[] object) {
        UUID uuidOfObject = UUID.nameUUIDFromBytes(object);
        ByteBuffer bb = ByteBuffer.wrap(new byte[20]);
        bb.putLong(uuidOfObject.getMostSignificantBits());
        bb.putLong(uuidOfObject.getLeastSignificantBits());
        return bb.array();
    }

    public boolean isThisServerResponsible(byte[] object) {
        int responsibleServerID = Math.abs(Arrays.hashCode(object) % serverCount);
        return this.serverID == responsibleServerID;
    }

    public void listen() {
        infinileapServer.listen();
    }
}
