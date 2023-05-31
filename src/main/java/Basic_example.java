//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.dse.DseCluster;
//import com.datastax.driver.dse.DseSession;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Paths;

public class Basic_example {
//    private DseSession session;
//    private DseCluster cluster;
    private CqlSession session;
    public static void main(String[] args) {

        Basic_example client = new Basic_example();
        try {

            client.connect();
//            MetricRegistry registry = client.session.getMetrics()
//                    .orElseThrow(() -> new IllegalStateException("Metrics are disabled"))
//                    .getRegistry();
//
//            JmxReporter reporter =
//                    JmxReporter.forRegistry(registry)
//                            .inDomain("com.datastax.oss.driver")
//                            .build();
//            reporter.start();
//            client.createSchema();
//            client.insertData(20);
            client.queryData();
//            client.dropKeyspace();
//            reporter.stop();
        }
        finally {
//            if (client.cluster != null) client.close();
            if (client.session != null) client.session.close();
        }
    }

    /**
     * Connect to cassandra
     */
    public void connect() {

//        // Create the CqlSession object:
//        try (CqlSession session = CqlSession.builder()
//                .withCloudSecureConnectBundle(Paths.get("C:\\Users\\Michael Maher\\Documents\\Projects\\secure-connect-mm2023.zip"))
//                .withAuthCredentials("YMtUZpoKSITuMNXaZmejFiFS","jDtaWXftWd5Bj.z.YgOlmZgkJAypvRhCSAEJNBkyYOudPDfkLCdtIojw0Lt1e-n--aoOf0PIx3TkKntyOj9Ny5P4DB_iQU,lzkp+_088KuIhlT_+w2dZpfu.HFL+p24L")
//                .build()) {
//            // Select the release_version from the system.local table:
//            ResultSet rs = session.execute("select release_version from system.local");
//            Row row = rs.one();
//            //Print the results of the CQL query to the console:
//            if (row != null) {
//                System.out.println(row.getString("release_version"));
//            } else {
//                System.out.println("An error occurred.");
//            }
//        }

//        String sessionConf = "src/main/resources/L1/application.conf";
//        String confFilePath = Paths.get(sessionConf).toAbsolutePath().toString();
//        session = CqlSession.builder()
//                .withConfigLoader(DriverConfigLoader.fromFile(new File(confFilePath)))
//                .build();

        // Using secure connect bundle
//        session = CqlSession.builder()
//                .withCloudSecureConnectBundle(Paths.get("/Users/mike.maher/Documents/dev/demo/secure-connect-ks1.zip"))
//                .withAuthCredentials("moImCNyzPsOuEEsLsiPCyilR","-,mnT_KmxMWiIf,Qeyxj3tbNMA-E42ohHqEDEwq3SAGvwO3PSCY1KoIiMbMkOcYTZTvGovAuDHyEQ.xbG.Z5H1_+3ub-qUch_3OeiNb9SFW--r2BBLjLukMZCLpgtsEJ")
//                .build();

        session = CqlSession.builder().addContactPoint(new InetSocketAddress("172.31.45.123", 9042)).build();
//        cluster = DseCluster.builder().addContactPoint("127.0.0.1").build();
//        session = cluster.connect();
//        System.out.printf("Connected session: %s%n", session.getCluster());
        System.out.printf("Connected session: %s%n", session.getContext());
        System.out.println("Connect to node.");
    }

    /**
     * Create the key space and table
     */
    public void createSchema() {
//        session.execute(
//                "CREATE KEYSPACE IF NOT EXISTS basic WITH replication "
//                        + "= {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute(
                "CREATE TABLE IF NOT EXISTS ks1.example ("
                        + "id int PRIMARY KEY,"
                        + "name text,"
                        + ");");
        System.out.println("Create keyspace and table.");
    }

    /**
     * Load a number of rows to the table
     */
    public void insertData(int size) {
        for (int i = 0; i < size; i++) {
            String query = String.format("INSERT INTO ks1.example(id, name) VALUES (%d, '%s');", i, Integer.toString(i));
            session.execute(query);
        }
        System.out.println("Insert data.");
    }

    /**
     * Query the data and print result
     */
    public void queryData() {
        String query = "SELECT * FROM basic.group_info_v1;";
//        String query = "SELECT * FROM ks1.example;";
        ResultSet results = session.execute(query);
        System.out.printf("%-20s\t%-20s%n", "account_number", "opco", "group_id__number");
        System.out.println("-----------------------+--------------------");
        for (Row row : results) {
            System.out.printf(
                    "%-20s\t%-20s%n",
                    row.getString("account_number"), row.getString("opco"), row.getString("group_id__number")
            );
//            System.out.printf(
//                    "%-20s\t%-20s%n",
//                    row.getInt("id"), row.getString("name")
//            );
        }
    }

    /**
     * Drop keyspace
     */
    public void dropKeyspace() {
        String query = "DROP Keyspace IF EXISTS basic;";
        session.execute(query);
        System.out.println("Drop keyspace and table.");
    }

    /**
     * Close session at the end
     */
    public void close() {
        if (session != null) {
            session.close();
            System.out.println("Close session.");
        }
    }
}