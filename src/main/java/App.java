import server.InfinimumDBServer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class App {
    public static void main(String[] args) {
        String plasmaFilePath = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";
        InfinimumDBServer server = new InfinimumDBServer(plasmaFilePath, "127.0.0.1", 2998);
        server.listen();

        String test = "test";
        System.out.println("Value to safe: " + test);
        byte[] object = test.getBytes();
        System.out.println("Is this server responsible: " + server.isThisServerResponsible(object));
        byte[] uuid = server.generateUUID(object);
        ByteBuffer bb = ByteBuffer.wrap(uuid);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        System.out.println("UUID of object: " + new UUID(firstLong, secondLong));
        server.put(uuid, object);

        byte[] value = server.get(uuid);
        System.out.println("Saved value was: " + new String(value, StandardCharsets.UTF_8));
    }
}
