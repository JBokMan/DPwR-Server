package server;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;

class DPwRServerTest {

    @Test
    void listen() {
        final InetSocketAddress listenAddress = new InetSocketAddress("127.0.0.1", 2998);
        final int plasmaStoreSize = 1000;
        final int plasmaTimeout = 500;
        final int clientTimeout = 500;
        final int workerCount = 256;
        final boolean verbose = false;
        final DPwRServer server = new DPwRServer(listenAddress, plasmaStoreSize, plasmaTimeout, clientTimeout, workerCount, verbose);
        server.listen();
    }
}