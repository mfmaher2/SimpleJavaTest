import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.net.InetSocketAddress;
import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

import java.util.concurrent.TimeUnit;
import java.security.SecureRandom;
import static java.lang.Math.toIntExact;



public class DriverMetricsExample {
    static MetricRegistry metrics = null;
    private CqlSession session;
    private static String createKeyspaceCQL = "create keyspace if not exists ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}";
    private static String createTableCQL = "create table if not exists ks.tbl (key int primary key, value text)";

    private static char[] lower = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String getRandomString(int length) {

        char[] c = new char[length];
        SecureRandom random = new SecureRandom();
        for (int i = 0; i < length; i++) {
            c[i] = lower[random.nextInt(lower.length)];
        }

        return new String(c);
    }


    private static void maybeCreateSchema(CqlSession _session){
        _session.execute(createKeyspaceCQL);
        _session.execute(createTableCQL);
    }

    private static int getCount(CqlSession _session){
//        SimpleStatement stmt = new SimpleStatement("select count(*) from ks.tbl");
        ResultSet rs = _session.execute(SimpleStatement.newInstance("select count(*) from ks.tbl"));
        int count = 0;
        for (Row row : rs) {
            count = toIntExact(row.getLong("count"));
        }
        return count;
    }

    private static void maybeWriteOneThousandRows(CqlSession _session) {

        int count = getCount(_session);

        if (count < 1000) {
            PreparedStatement prepStmt = _session.prepare("INSERT INTO ks.tbl (key, value) values (?,?)");
            for (int i = 1; i < 1001; i++){
                _session.executeAsync(prepStmt.bind(i, getRandomString(10)));
            }
        }

        int after_count = getCount(_session);

        if (after_count < 1000){
            System.out.println(String.format("Did not find 1000 rows as expected, found %d", after_count));
            System.exit(1);
        }
    }

    private static void queryForDuration(CqlSession _session, String durationInSeconds, String waitSecondsAfterQuery) {
        queryForDuration(_session, Integer.parseInt(durationInSeconds), Integer.parseInt(waitSecondsAfterQuery));
    }

    private static void queryForDuration(CqlSession _session, int durationInSeconds, int waitSecondsAfterQuery) {
        PreparedStatement prepStmt = _session.prepare("select value from ks.tbl where key = ?");
        int runTime = 0;
        int count = 0;

        while (runTime < durationInSeconds) {
            int randomKey = ThreadLocalRandom.current().nextInt(1, 1000 + 1);
            ResultSet rs = _session.execute(prepStmt.bind(randomKey));
            System.out.println(String.format("Query %d - key:%d | value:%s", count, randomKey, rs.one().getString("value")));
            count += 1;
            runTime += waitSecondsAfterQuery;

            try {
                Thread.sleep(waitSecondsAfterQuery * 1000);
            }
            catch (InterruptedException e){
                System.out.println("Interrupted while sleeping between queries, exiting");
                System.exit(1);
            }
        }
        System.out.println();
        System.out.println(String.format("Queried %d times in %d seconds", count, runTime));
        System.out.println();
    }

    private static ImmutableMap getArgsMap(String[] args){
        return ImmutableMap.of("ip", args[0], "dc", args[1], "duration", args[2], "wait", args[3]);
    }

    private static void startGraphiteReporting(CqlSession _session){

        Graphite graphite = new Graphite(new InetSocketAddress("127.0.0.1", 2003));
        GraphiteReporter reporter = GraphiteReporter
                .forRegistry(getMetrics())
                .prefixedWith("chris-java-driver-metrics")
                .build(graphite);
        reporter.start(1, TimeUnit.SECONDS);
    }

    synchronized void initIfNecessary() {
        if (metrics == null) {
            metrics = new MetricRegistry();
//            metrics.registerAll(new MemoryUsageGaugeSet());
//            metrics.registerAll(new GarbageCollectorMetricSet());
        }
    }

    public static synchronized MetricRegistry getMetrics() {
        return metrics;
    }

    public static void main(String[] args) throws InterruptedException {
//        DriverMetricsExample client = new DriverMetricsExample();
//        client.connect();
////        ImmutableMap argsMap = getArgsMap(args);
////        String contactPoint = argsMap.get("ip").toString();
////        String dc = argsMap.get("dc").toString();
        CqlSession session = CqlSession.builder().build();
//        maybeCreateSchema(session);
//        maybeWriteOneThousandRows(session);
//        startGraphiteReporting(session);
////        queryForDuration(session, argsMap.get("duration").toString(), argsMap.get("wait").toString());
//        System.exit(0);


        String contactPoint = System.getProperty("contactPoint", "127.0.0.1");
//        try (CqlSession session = CqlSession.builder()
////                .addContactPoint(new InetSocketAddress(contactPoint, 9042))
//                .withLocalDatacenter(System.getProperty("SearchGraphAnalytics"))
//                .build()) {

        MetricRegistry registry = session.getMetrics()
                .orElseThrow(() -> new IllegalStateException("Metrics are disabled"))
                .getRegistry();
//            CollectorRegistry.defaultRegistry.register(new DropwizardExports(registry));
            Graphite graphite = new Graphite(new InetSocketAddress("127.0.0.1", 2003));
            GraphiteReporter reporter = GraphiteReporter
                    .forRegistry(registry)
                    .prefixedWith("chris-java-driver-metrics")
                    .build(graphite);

            session.execute("create keyspace if not exists test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            session.execute("create table if not exists test.abc (id int, t1 text, t2 text, primary key (id, t1));");
            session.execute("truncate test.abc;");

            for(int i = 0; i < 3000; i++) {
                for(int j = 0; j < 1000; j++) {
                    session.executeAsync(String.format("insert into test.abc (id, t1, t2) values (%d, 't1-%d', 't2-%d');",
                            i+j, i, j));
                }
                Thread.sleep(5000);
            }
//        }
    }

    private void connect() {

        session = CqlSession.builder().build();
        System.out.printf("Connected session: %s%n", session.getContext());
        System.out.println("Connect to node.");
    }


}