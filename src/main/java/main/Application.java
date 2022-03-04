package main;

import server.InfinimumDBServer;

public class Application {
    private final static String PLASMA_FILE_PATH = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";

    public static void main(final String[] args) {
        final InfinimumDBServer server = new InfinimumDBServer(PLASMA_FILE_PATH, "127.0.0.1", 2998);
        server.listen();
    }
}

