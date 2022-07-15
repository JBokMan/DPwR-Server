package server;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

class DPwRServerTest {

    @Test
    void listen() {
        final InetSocketAddress listenAddress = new InetSocketAddress("127.0.0.1", 2998);
        final int plasmaStoreSize = 1000;
        final int plasmaTimeout = 500;
        final int clientTimeout = 500;
        final int workerCount = 8;
        final boolean verbose = false;
        final DPwRServer server = new DPwRServer(listenAddress, plasmaStoreSize, clientTimeout, workerCount, verbose);
        server.listen();
    }
}