package server;

import jep.Interpreter;
import jep.SharedInterpreter;

public class InfinimumDBServer {

    private final Interpreter interpreter = new SharedInterpreter();
    private final String plasmaFilePath;

    public InfinimumDBServer(String plasmaFilePath, Integer listeningPort) {
        this.plasmaFilePath = plasmaFilePath;
        initializeServer(listeningPort);
    }

    public InfinimumDBServer(String plasmaFilePath, Integer plasmaPort, Integer listeningPort,
                             String mainServerHostAddress, Integer mainServerPort) {
        this.plasmaFilePath = plasmaFilePath;
        initializeSecondaryServer(listeningPort, mainServerHostAddress, mainServerPort);
    }

    private void initializeSecondaryServer(Integer listeningPort,
                                           String mainServerHostAddress, Integer mainServerPort) {
        // TODO implement
        initializeServer(listeningPort);
    }

    private void initializeServer(Integer listeningPort) {
        connectPlasma();
    }

    private void connectPlasma() {
        try {
            interpreter.exec("import pyarrow.plasma as plasma");
            interpreter.exec("client = plasma.connect(\"" + this.plasmaFilePath + "\")");
        } catch (Exception e) {
            System.err.println("Plasma server not reachable");
        }
    }

    public Object putString(String string) {
        try {
            interpreter.exec("object_id = client.put(\"" + string + "\")");
            return interpreter.getValue("object_id", Object.class);
        } catch (Exception e) {
            System.err.println("Plasma server no longer reachable, trying to reconnect");
            try {
                connectPlasma();
                return putString(string);
            } catch (Exception ex) {
                return null;
            }
        }
    }

    public String getString(Object id) {
        try {
            interpreter.set("object_id", id);
            return interpreter.getValue("client.get(object_id)", String.class);
        } catch (Exception e) {
            System.err.println("Plasma server no longer reachable, trying to reconnect");
            connectPlasma();
            return null;
        }
    }

}
