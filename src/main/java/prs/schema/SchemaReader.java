package prs.schema;

import prs.db.DbType;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class SchemaReader {

    private final DataSource ds;
    private final DbType dbType;

    public SchemaReader(DataSource ds, DbType dbType) {
        this.ds = ds;
        this.dbType = dbType;
    }

    public List<String> getAllTables(String dbName) throws SQLException {
        List<String> tables = new ArrayList<>();

        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();

            String catalogArg;
            String schemaPattern;

            if (dbType == DbType.MYSQL) {
                catalogArg    = dbName;
                schemaPattern = null;
            } else {
                catalogArg    = null;
                schemaPattern = "public";
            }

            try (ResultSet rs = meta.getTables(catalogArg, schemaPattern, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    tables.add(tableName);
                }
            }
        }

        return tables;
    }

    public Map<String, List<String>> getTableDependencies(String dbName) throws SQLException {
        Map<String, List<String>> dependencies = new HashMap<>();
        List<String> allTables = getAllTables(dbName);

        for (String tableOrig : allTables) {
            dependencies.put(tableOrig.toLowerCase(), new ArrayList<>());
        }

        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();

            for (String tableOrig : allTables) {
                String tableLC = tableOrig.toLowerCase();

                String catalogArg;
                String schemaPattern;
                if (dbType == DbType.MYSQL) {
                    catalogArg    = dbName;
                    schemaPattern = null;
                } else {
                    catalogArg    = null;
                    schemaPattern = "public";
                }

                try (ResultSet rs = meta.getImportedKeys(catalogArg, schemaPattern, tableOrig)) {
                    while (rs.next()) {
                        String parentTableOrig = rs.getString("PKTABLE_NAME");
                        dependencies.get(tableLC).add(parentTableOrig.toLowerCase());
                    }
                }
            }
        }

        return dependencies;
    }

    public Map<String, String> getForeignKeys(String dbName, String tableNameOrig) throws SQLException {
        Map<String, String> fkMap = new HashMap<>();

        try (Connection conn = ds.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();

            String catalogArg;
            String schemaPattern;
            if (dbType == DbType.MYSQL) {
                catalogArg    = dbName;
                schemaPattern = null;
            } else {
                catalogArg    = null;
                schemaPattern = "public";
            }

            try (ResultSet rs = meta.getImportedKeys(catalogArg, schemaPattern, tableNameOrig)) {
                while (rs.next()) {
                    String fkColumnOrig = rs.getString("FKCOLUMN_NAME");
                    String fkColumnLC   = fkColumnOrig.toLowerCase();
                    String pkTableOrig  = rs.getString("PKTABLE_NAME");
                    String pkTableLC    = pkTableOrig.toLowerCase();
                    fkMap.put(fkColumnLC, pkTableLC);
                }
            }
        }

        return fkMap;
    }
}
