package main;

import de.hhu.bsinfo.infinileap.example.util.Constants;
import de.hhu.bsinfo.infinileap.example.util.InetSocketAddressConverter;
import org.apache.commons.lang3.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import server.DPwRServer;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

@Command(name = "dpwr_server", mixinStandardHelpOptions = true,
        description = "Starts a DPwR server with the given address and port")
public class Application implements Callable<Integer> {
    @Option(names = {"-l", "--listen"}, description = "The address the server should listen on. Default is 127.0.0.1:2998")
    private final InetSocketAddress listenAddress = new InetSocketAddress("127.0.0.1", 2998);
    @Option(names = {"-c", "--connect"}, defaultValue = Option.NULL_VALUE, description = "The address of the main server this server should connect to. Default is null")
    private InetSocketAddress mainServerAddress;
    @Option(names = {"-s", "--size"}, description = "The space that the plasma store should reserve in megabytes. Default is 1000MB")
    private int plasmaStoreSize = 1000;
    @Option(names = {"-p", "--plasma-timeout"}, description = "The timeout for plasma operations in milliseconds. Default is 500MS")
    private int plasmaTimeout = 500;
    @Option(names = {"-t", "--client-timeout"}, description = "The timeout for client operations in milliseconds. Default is 500MS")
    private int clientTimeout = 500;
    @Option(names = {"-v", "--verbose"}, description = "Whether or not info logs should be displayed. Default is false")
    private boolean verbose = false;

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new Application())
                .registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(Constants.DEFAULT_PORT))
                .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        final DPwRServer server;
        try {
            if (ObjectUtils.isEmpty(mainServerAddress)) {
                server = new DPwRServer(listenAddress, plasmaStoreSize, plasmaTimeout, clientTimeout, verbose);
            } else {
                server = new DPwRServer(listenAddress, mainServerAddress, plasmaStoreSize, plasmaTimeout, clientTimeout, verbose);
            }
            server.listen();
            return 0;
        } catch (final Exception e) {
            return -1;
        }
    }
}

