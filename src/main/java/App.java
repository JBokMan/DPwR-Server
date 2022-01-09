import org.apache.arrow.plasma.PlasmaClient;
import server.InfinimumDBServer;

public class App {
    public static void main(String[] args) {
        //InfinimumDBServer server = new InfinimumDBServer("/home/julian/Documents/Masterarbeit/Plasma-Examples/plasma", 4321);

        /*Object id = server.putString("Bla");
        System.out.println(server.getString(id));

        Thread.sleep(5000);

        Object id2 = server.putString("Bla2");
        System.out.println(server.getString(id2));
        */
        System.loadLibrary("plasma_java");
        String plasmaFilePath = "/home/julian/Documents/Masterarbeit/Plasma-Examples/plasma";
        PlasmaClient client = new PlasmaClient(plasmaFilePath, "", 0);
    }
}
