package prs.filler;

import prs.db.DbType;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class DataFiller {

    private final DataSource ds;
    private final DbType dbType;

    public DataFiller(DataSource ds, DbType dbType) {
        this.ds = ds;
        this.dbType = dbType;
    }

    public List<Object> fillTable(String tableName, List<Map<String, Object>> rows) throws SQLException {
        List<Object> generatedKeys = new ArrayList<>();
        if (rows.isEmpty()) {
            System.out.println("fillTable: Nema redova za ubaciti u tablicu " + tableName);
            return generatedKeys;
        }

        List<String> columns = new ArrayList<>(rows.get(0).keySet());

        char q = (dbType == DbType.MYSQL) ? '`' : '"';

        List<String> quotedColumns = new ArrayList<>();
        for (String col : columns) {
            quotedColumns.add(q + col + q);
        }
        String colNamesQuoted = String.join(", ", quotedColumns);

        String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));

        String sql = "INSERT INTO " + q + tableName + q + " (" + colNamesQuoted + ") VALUES (" + placeholders + ")";

        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            // batch
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < columns.size(); i++) {
                    stmt.setObject(i + 1, row.get(columns.get(i)));
                }
                stmt.addBatch();
            }

            // exec batch i klk je redova ubaceno
            int[] updateCounts = stmt.executeBatch();
            int rowsAffected = 0;
            for (int count : updateCounts) {
                if (count > 0) {
                    rowsAffected += count;
                } else if (count == Statement.SUCCESS_NO_INFO) {
                    rowsAffected += 1;
                }
            }
            System.out.println("fillTable: U tablicu \"" + tableName + "\" ubaceno (prema updateCounts) = "
                    + rowsAffected + " redova.");

            try (ResultSet rsKeys = stmt.getGeneratedKeys()) {
                while (rsKeys.next()) {
                    generatedKeys.add(rsKeys.getObject(1));
                }
            }
            System.out.println("fillTable: Dobijeno auto-generated kljuceva = "
                    + generatedKeys.size() + " za tablicu \"" + tableName + "\".");

        } catch (SQLException e) {
            System.err.println("fillTable: SQL Exception pri ubacivanju u \""
                    + tableName + "\": " + e.getMessage());
            throw e;
        }

        return generatedKeys;
    }
}
