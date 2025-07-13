package prs.data;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class DataGenerator {

    private final DataSource ds;

    public DataGenerator(DataSource ds) {
        this.ds = ds;
    }
    public List<Map<String, Object>> generateData(
            String tableNameOrig,
            int rowCount,
            Map<String, List<Object>> knownValues,
            Map<String, String> foreignKeys) throws SQLException {

        // podaci o atributima i pk
        List<ColumnInfo> columns;
        List<String> pkColsOrig;
        try (Connection conn = ds.getConnection()) {
            columns = getTableColumns(conn, tableNameOrig);
            pkColsOrig = getPrimaryKeys(conn, tableNameOrig);
        }

        // pk to lower
        List<String> pkColsLC = new ArrayList<>();
        for (String pk : pkColsOrig) {
            pkColsLC.add(pk.toLowerCase());
        }

        // za moj slucaj kad je pk sastavljen samo od fk
        boolean isCompositePK_AllFK = false;
        if (pkColsLC.size() > 1) {
            isCompositePK_AllFK = true;
            for (String pkLC : pkColsLC) {
                if (!foreignKeys.containsKey(pkLC)) {
                    isCompositePK_AllFK = false;
                    break;
                }
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();

        if (isCompositePK_AllFK) {
            // pk=komb fkova poznatih
            String parentA = foreignKeys.get(pkColsLC.get(0));
            String parentB = foreignKeys.get(pkColsLC.get(1));
            List<Object> parentValsA = knownValues.getOrDefault(parentA, Collections.emptyList());
            List<Object> parentValsB = knownValues.getOrDefault(parentB, Collections.emptyList());

            if (parentValsA.isEmpty() || parentValsB.isEmpty()) {
                return result; // nisu upani parenti
            }

            int maxComb = parentValsA.size() * parentValsB.size();
            int desired = Math.min(rowCount, maxComb);

            Set<String> usedPairs = new HashSet<>(desired);
            Random rand = new Random();

            while (result.size() < desired) {
                Object a = parentValsA.get(rand.nextInt(parentValsA.size()));
                Object b = parentValsB.get(rand.nextInt(parentValsB.size()));
                String key = a.toString() + "||" + b.toString();
                if (usedPairs.contains(key)) {
                    continue;
                }
                usedPairs.add(key);

                Map<String, Object> row = new LinkedHashMap<>();
                // slozeni pk
                row.put(pkColsOrig.get(0), a);
                row.put(pkColsOrig.get(1), b);

                // generisanje ostalih kolona, neputpuno skroz, prilagodjeno samo za moju semu
                for (ColumnInfo col : columns) {
                    String colNameOrig = col.name;
                    String colLC = colNameOrig.toLowerCase();

                    if (col.isAutoIncrement || pkColsLC.contains(colLC)) {
                        continue;
                    }

                    Object value;
                    if (foreignKeys.containsKey(colLC)) {
                        String parentTableLC = foreignKeys.get(colLC);
                        List<Object> parentList = knownValues.getOrDefault(parentTableLC, Collections.emptyList());
                        value = parentList.isEmpty()
                                ? null
                                : parentList.get(rand.nextInt(parentList.size()));
                    } else {
                        value = generateRandomValue(colNameOrig, col.type, !col.nullable);
                    }
                    row.put(colNameOrig, value);
                }

                result.add(row);
            }

            return result;
        }

        // standardni slucaj: pk sa jednom vr ili bez slozenih fk
        Random rand = new Random();
        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> row = new LinkedHashMap<>();

            for (ColumnInfo col : columns) {
                if (col.isAutoIncrement) {
                    continue;
                }
                String colLC = col.name.toLowerCase();
                Object value;

                if (foreignKeys.containsKey(colLC)) {
                    String parentTableLC = foreignKeys.get(colLC);
                    List<Object> parentVals = knownValues.getOrDefault(parentTableLC, Collections.emptyList());
                    if (!parentVals.isEmpty()) {
                        value = parentVals.get(rand.nextInt(parentVals.size()));
                    } else {
                        value = null;
                    }
                } else {
                    value = generateRandomValue(col.name, col.type, !col.nullable);
                }
                row.put(col.name, value);
            }

            result.add(row);
        }

        return result;
    }

    private List<String> getPrimaryKeys(Connection conn, String tableNameOrig) throws SQLException {
        List<String> pks = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(null, null, tableNameOrig)) {
            while (rs.next()) {
                pks.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pks;
    }

    private List<ColumnInfo> getTableColumns(Connection conn, String tableNameOrig) throws SQLException {
        List<ColumnInfo> cols = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, tableNameOrig, null)) {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                int type = rs.getInt("DATA_TYPE");
                boolean nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
                boolean autoInc = "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT"));
                cols.add(new ColumnInfo(name, type, nullable, autoInc));
            }
        }
        return cols;
    }

    private Object generateRandomValue(String columnName, int columnType, boolean required) {
        Random rand = new Random();

        switch (columnType) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                if (columnName.equalsIgnoreCase("Opis") || columnName.equalsIgnoreCase("UputeZaPripremu")) {
                    return "Automatski tekst: " + generateRandomString(20);
                }
                return generateRandomString(10);

            case Types.INTEGER:
            case Types.SMALLINT:
                return rand.nextInt(1_000_000);

            case Types.TINYINT:
                // MySQL TINYINT(1) se koristi kao boolean, vraća 0 ili 1w
                return rand.nextInt(2);

            case Types.BOOLEAN:
            case Types.BIT:
                // PostgreSQL BOOLEAN moze biti kao BIT; vraca pravi boolean
                return rand.nextBoolean();

            case Types.DECIMAL:
            case Types.NUMERIC:
                return Math.round(rand.nextDouble() * 10_000.0) / 100.0;

            case Types.DATE:
                return new java.sql.Date(System.currentTimeMillis() - rand.nextInt(1_000_000_000));

            case Types.TIMESTAMP:
                return new java.sql.Timestamp(System.currentTimeMillis() - rand.nextInt(1_000_000_000));

            default:
                return required ? "DefaultVal" : null;
        }
    }

    private String generateRandomString(int length) {
        String letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(letters.charAt(r.nextInt(letters.length())));
        }
        return sb.toString();
    }

    private static class ColumnInfo {
        String name;
        int type;
        boolean nullable;
        boolean isAutoIncrement;

        ColumnInfo(String name, int type, boolean nullable, boolean isAutoIncrement) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
            this.isAutoIncrement = isAutoIncrement;
        }
    }
}
