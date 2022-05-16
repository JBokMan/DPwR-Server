package main;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import server.DPwRServer;

import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

public class ApplicationSecondary {
    public static void main(final String[] args) throws ControlException, TimeoutException, ConnectException {
        final DPwRServer server = new DPwRServer("127.0.0.1", 2997, "127.0.0.1", 2998);
        server.listen();
    }
}

