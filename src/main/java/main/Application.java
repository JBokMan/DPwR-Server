package main;

import server.DPwRServer;

public class Application {
    public static void main(final String[] args) {
        final DPwRServer server = new DPwRServer("127.0.0.1", 2998);
        server.listen();
    }
}

