package main;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import server.DPwRServer;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;

public class ApplicationSecondary {
    private final static String PLASMA_FILE_PATH = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";

    public static void main(final String[] args) throws UnknownHostException, ControlException, TimeoutException, ConnectException {
        final DPwRServer server = new DPwRServer(PLASMA_FILE_PATH, "127.0.0.1", 2997, "127.0.0.1", 2998);
    }
}

