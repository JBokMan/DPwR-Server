package main;

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.SerializationUtils;
import server.InfinimumDBServer;

import java.nio.ByteBuffer;

public class Application {
    private final static String PLASMA_FILE_PATH = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";

    public static void main(final String[] args) {
        final InfinimumDBServer server = new InfinimumDBServer(PLASMA_FILE_PATH, "127.0.0.1", 2998);
        server.listen();
        //putTest();
        //deleteTest();
        //createAndSealTest();
        //putAndDeleteTest();
        //createAndSealAndDeleteTest();
        //putAndImmediateDeleteTest();
        //createAndSealAndImmediateDeleteTest();
    }

    // Put test
    public static void putTest() {
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient(PLASMA_FILE_PATH, "", 0);
        byte[] id = new byte[20];
        byte[] object = SerializationUtils.serialize("test");
        byte[] metadata = new byte[0];
        plasmaClient.put(id, object, metadata);
    }
    /* Result:
  {ObjectID(0000000000000000000000000000000000000000): {'data_size': 11,
  'metadata_size': 0,
  'ref_count': 0,
  'create_time': 1645972427,
  'construct_duration': 0,
  'state': 'sealed'}} */

    // Delete test
    public static void deleteTest() {
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient(PLASMA_FILE_PATH, "", 0);
        byte[] id = new byte[20];
        plasmaClient.delete(id);
    }
    /* Result:
    after running Put Test: {}
    after running CreateAndSeal Test: {}
     */


    public static void createAndSealTest() {
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient(PLASMA_FILE_PATH, "", 0);
        byte[] id = new byte[20];
        byte[] object = SerializationUtils.serialize("test");
        int objectSize = object.length;
        byte[] metadata = new byte[0];
        ByteBuffer byteBuffer = plasmaClient.create(id, objectSize, metadata);
        for (byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
    }
    /* Result:
    {ObjectID(0000000000000000000000000000000000000000): {'data_size': 11,
  'metadata_size': 0,
  'ref_count': 0,
  'create_time': 1645972764,
  'construct_duration': 0,
  'state': 'sealed'}}
     */

    public static void putAndDeleteTest() {
        putTest();
        deleteTest();
    }
    /* Result:
    {}
     */

    public static void createAndSealAndDeleteTest() {
        createAndSealTest();
        deleteTest();
    }
    /* Result:
    {}
     */

    public static void putAndImmediateDeleteTest() {
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient(PLASMA_FILE_PATH, "", 0);
        byte[] id = new byte[20];
        byte[] object = SerializationUtils.serialize("test");
        byte[] metadata = new byte[0];
        plasmaClient.put(id, object, metadata);
        plasmaClient.delete(id);
    }
    /* Result:
    {}
     */

    public static void createAndSealAndImmediateDeleteTest() {
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient(PLASMA_FILE_PATH, "", 0);
        byte[] id = new byte[20];
        byte[] object = SerializationUtils.serialize("test");
        int objectSize = object.length;
        byte[] metadata = new byte[0];
        ByteBuffer byteBuffer = plasmaClient.create(id, objectSize, metadata);
        for (byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
        plasmaClient.delete(id);
    }
    /* Result:
    {ObjectID(0000000000000000000000000000000000000000): {'data_size': 11,
  'metadata_size': 0,
  'ref_count': 0,
  'create_time': 1645973202,
  'construct_duration': 0,
  'state': 'sealed'}}
     */
}

