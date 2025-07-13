package prs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import prs.db.DbType;
import prs.graph.DependencyGraph;
import prs.schema.SchemaReader;
import prs.data.DataGenerator;
import prs.filler.DataFiller;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);

        System.out.println("Unesite vrstu (unesite broj):");
        System.out.println("1) MySQL (lokalna)");
        System.out.println("2) MySQL (Aiven - udaljeni server)");
        System.out.println("3) PostgreSQL (lokalna)");
        System.out.println("4) PostgreSQL (Aiven – udaljeni server)");
        int izbor = Integer.parseInt(sc.nextLine().trim());

        DbType dbType;
        String host;
        int port;
        String dbName;
        String user;
        String pass;

        switch (izbor) {
            case 1 -> {
                // MySQL local
                dbType = DbType.MYSQL;
                host   = "localhost";
                port   = 3306;
                dbName = "prsprojlocal";
                user   = "root";
                pass   = "placeholder";
            }
            case 2 -> {
                // MySQL Aiven remote
                dbType = DbType.MYSQL;
                host   = "prsprojekat-prsprojekat.g.aivencloud.com";
                port   = 15406;
                dbName = "mydb";
                user   = "avnadmin";
                pass   = "placeholder";
            }
            case 3 -> {
                // PostgreSQL local
                dbType = DbType.POSTGRESQL;
                host   = "localhost";
                port   = 5432;
                dbName = "prsprojlocalPOSTGRESQL";
                user   = "postgres";
                pass   = "placeholder";
            }
            case 4 -> {
                // PostgreSQL Aiven remote
                dbType = DbType.POSTGRESQL;
                host   = "prsprojekat-postgresql-prsprojekat.g.aivencloud.com";
                port   = 15406;
                dbName = "mydb";
                user   = "avnadmin";
                pass   = "placeholder";
            }
            default -> {
                System.err.println("pogresan odabir");
                return;
            }
        }

        HikariConfig config = new HikariConfig();
        String jdbcUrl;
        if (dbType == DbType.MYSQL) {
            jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + dbName +
                    "?useSSL=true&requireSSL=true&serverTimezone=UTC";
            config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        } else {
            jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            if (!host.equalsIgnoreCase("localhost")) {
                jdbcUrl += "?sslmode=require";
            }
            config.setDriverClassName("org.postgresql.Driver");
        }
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(pass);

        config.setMaximumPoolSize(15);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30_000);
        config.setIdleTimeout(300_000);
        config.setMaxLifetime(1_500_000);

        HikariDataSource dataSource = new HikariDataSource(config);

        // check
        try (Connection testConn = dataSource.getConnection()) {
            System.out.println("Uspjesna konekcija preko HikariCP poola");
        } catch (Exception e) {
            System.err.println("Neuspjesna konekcija preko HCP poola" + e.getMessage());
            dataSource.close();
            return;
        }

        SchemaReader reader = new SchemaReader(dataSource, dbType);

        List<String> allTablesOrig = reader.getAllTables(dbName);
        if (allTablesOrig.isEmpty()) {
            System.err.println("Nema pronadnjenih tablica u bazi \"" + dbName + "\". Provjerite parametre");
            dataSource.close();
            return;
        }

        Map<String, String> lcToOriginal = new HashMap<>();
        for (String orig : allTablesOrig) {
            lcToOriginal.put(orig.toLowerCase(), orig);
        }

        System.out.println("\nTabele u bazi:");
        allTablesOrig.forEach(t -> System.out.println("  - " + t));

        Map<String, List<String>> deps = reader.getTableDependencies(dbName);

        List<String> allTablesLC = new ArrayList<>(lcToOriginal.keySet());
        DependencyGraph graph = new DependencyGraph(deps);
        List<String> sortedTablesLC;
        try {
            sortedTablesLC = graph.topologicalSort(allTablesLC);
        } catch (RuntimeException e) {
            System.err.println("Greska: ciklicne zavisnosti medju tabelama " + e.getMessage());
            dataSource.close();
            return;
        }

        System.out.println("\nRedoslijed za punjenje (lower-case):");
        sortedTablesLC.forEach(t -> System.out.println("  - " + t));

        //level za svaku tablicu 0->nema roditelja moze se popunjavati sama, 1->zavisi od nivoa 0...
        Map<String, Integer> levelMap = computeLevels(sortedTablesLC, deps);

        Map<Integer, List<String>> tablesByLevel = new TreeMap<>();
        for (String tableLC : sortedTablesLC) {
            int lvl = levelMap.getOrDefault(tableLC, 0);
            tablesByLevel.computeIfAbsent(lvl, k -> new ArrayList<>()).add(tableLC);
        }

        Map<String, List<Object>> knownValues = new ConcurrentHashMap<>();

        //int maxThreads = Math.min(Runtime.getRuntime().availableProcessors(), 15);
        ExecutorService executor = Executors.newFixedThreadPool(15);

        long overallStart = System.currentTimeMillis();

        for (Map.Entry<Integer, List<String>> entry : tablesByLevel.entrySet()) {
            List<String> levelTablesLC = entry.getValue();
            List<Future<?>> futures = new ArrayList<>();

            for (String tableLC : levelTablesLC) {
                String tableOrig = lcToOriginal.get(tableLC);

                Future<?> future = executor.submit(() -> {
                    try {
                        System.out.println("\nPopunjavanje tabele: " + tableOrig);
                        long start = System.currentTimeMillis();

                        SchemaReader threadReader = new SchemaReader(dataSource, dbType);
                        DataGenerator threadGen   = new DataGenerator(dataSource);
                        DataFiller threadFill = new DataFiller(dataSource, dbType);


                        Map<String, String> foreignKeys = threadReader.getForeignKeys(dbName, tableOrig);

                        List<Map<String, Object>> data =
                                threadGen.generateData(tableOrig, 10_000, knownValues, foreignKeys);

                        List<Object> genKeys = threadFill.fillTable(tableOrig, data);

                       if (!genKeys.isEmpty()) {
                            knownValues.put(tableLC, genKeys);
                        } else if (!data.isEmpty()) {
                            String firstCol = data.get(0).keySet().iterator().next();
                            List<Object> vals = new ArrayList<>();
                            for (Map<String, Object> row : data) {
                                Object v = row.get(firstCol);
                                if (v != null) {
                                    vals.add(v);
                                }
                            }
                            knownValues.put(tableLC, vals);
                        }

                        long end = System.currentTimeMillis();
                        System.out.println("Tabela " + tableOrig +
                                " popunjena u " + (end - start) + " ms " +
                                "(ubaceno " + (genKeys.isEmpty() ? 0 : genKeys.size()) + " redova)");
                    } catch (Exception e) {
                        System.out.println("Greska pri popunjavanju tabele " +
                                tableOrig + ": " + e.getMessage());
                    }
                });

                futures.add(future);
            }

            // sve niti sa cur nivoa moraju finish
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException ignored) {
                }
            }
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long overallEnd = System.currentTimeMillis();
        System.out.println("\nPopunjavanje baze zavrseno. Ukupno vrijeme: " +
                (overallEnd - overallStart) + " ms");

        dataSource.close();
    }

    private static Map<String, Integer> computeLevels(
            List<String> sortedTablesLC,
            Map<String, List<String>> deps) {

        Map<String, Integer> levelMap = new HashMap<>();

        for (String tableLC : sortedTablesLC) {
            List<String> parents = deps.getOrDefault(tableLC, Collections.emptyList());
            int maxParentLevel = -1;
            for (String p : parents) {
                int pl = levelMap.getOrDefault(p.toLowerCase(), 0);
                if (pl > maxParentLevel) {
                    maxParentLevel = pl;
                }
            }
            levelMap.put(tableLC, maxParentLevel + 1);
        }

        return levelMap;
    }
}
