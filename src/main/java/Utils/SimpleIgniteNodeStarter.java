package Utils;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class SimpleIgniteNodeStarter {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {

            while (true) {
            }
        }

    }
}
