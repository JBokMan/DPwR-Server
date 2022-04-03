package main;

import server.DPwRServer;

import java.net.UnknownHostException;

public class Application {
    private final static String PLASMA_FILE_PATH = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";

    public static void main(final String[] args) throws UnknownHostException {
        final DPwRServer server = new DPwRServer(PLASMA_FILE_PATH, "127.0.0.1", 2998);
        server.listen();
    }
}

