package server;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeoutException;

class DPwRServerTest {

    @Test
    void listenAsMain() {
        final InetSocketAddress listenAddress = new InetSocketAddress("127.0.0.1", 2998);
        final int plasmaStoreSize = 1000;
        final int clientTimeout = 500;
        final int workerCount = 8;
        final boolean verbose = true;
        final DPwRServer server = new DPwRServer(listenAddress, plasmaStoreSize, clientTimeout, workerCount, verbose);
        server.listen();
    }

    @Test
    void listenAsSecondary() throws ControlException, TimeoutException, ConnectException {
        final InetSocketAddress listenAddress = new InetSocketAddress("127.0.0.1", 2997);
        final int plasmaStoreSize = 1000;
        final int clientTimeout = 500;
        final int workerCount = 8;
        final boolean verbose = true;
        final InetSocketAddress mainServerAddress = new InetSocketAddress("127.0.0.1", 2998);
        final DPwRServer server = new DPwRServer(listenAddress, mainServerAddress, plasmaStoreSize, clientTimeout, workerCount, verbose);
        server.listen();
    }
}