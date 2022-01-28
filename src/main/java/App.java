import server.InfinimumDBServer;

public class App {
    public static void main(String[] args) {
        final String plasmaFilePath = "/home/julian/Documents/Masterarbeit/InfinimumDB-Server/plasma";
        final InfinimumDBServer server = new InfinimumDBServer(plasmaFilePath, "127.0.0.1", 2998);
        server.listen();
    }
}
