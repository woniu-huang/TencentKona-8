package org.example;

import org.apache.commons.dbcp2.BasicDataSource;
import org.openjdk.jmh.annotations.*;

import java.sql.*;
import java.util.concurrent.*;


@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class DatabaseSyncPerformanceComparison {

//    private static final int NUM_TASKS = 10000;
//    private static final int NUM_THREADS = 1000;

    private static BasicDataSource dataSource;

    private static Thread.Builder.OfVirtual builder;

    @Setup(Level.Trial)
    public void setup() {

        if( testOption == 0){
            builder = Thread.ofVirtual().scheduler(Executors.newFixedThreadPool(threadCount));
        } else if( testOption == 1){
            builder = Thread.ofVirtual().scheduler(new ForkJoinPool(threadCount));
        }


        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername("root");
//        dataSource.setUrl("jdbc:mysql://localhost:3306/testdb");
//        dataSource.setPassword("q19723011");
        dataSource.setPassword("123456");
        dataSource.setUrl("jdbc:mysql://localhost:3306/hsb");
    }

    @TearDown(Level.Trial)
    public void teardown() {
        try {
            dataSource.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Param({"0", "1"})
    public int testOption;

    @Param({"1000", "5000", "10000"})
    public int requestCount;

    @Param({"100", "500",  "5000"})
    public int threadCount;



    public static String execQuery(String sql) throws SQLException {
        String queryResult = "";
        Statement statement = null;
        try (Connection connection = dataSource.getConnection()) {

            statement = connection.createStatement();

            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                int id = rs.getInt("id");
                String username = rs.getString("username");
                String email = rs.getString("email");
                queryResult = "id: " + id + " username: " + username + " email: " + email + "\n";
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            statement.close();
        }
        return queryResult;
    }

    @Benchmark
    public void testDB() throws Exception {
        Thread thread = builder.start(() -> {
            CompletableFuture[] futures = new CompletableFuture[requestCount];
            String sql = "SELECT * FROM users ";
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        execQuery(sql);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            for (int i = 0; i < requestCount; i++) {
                futures[i] = CompletableFuture.runAsync(r);
            }
            try {
                CompletableFuture.allOf(futures).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.join();

    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}