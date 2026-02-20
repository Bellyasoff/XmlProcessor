package io.bellyasoff;

import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.Node;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author IlyaB
 */
public class Service {

    private final Connection connection;
    private final GPathResult xml;

    public Service(String xmlUrl, Connection connection) {
        this.connection = connection;

        XmlLoader xmlLoader = new XmlLoader();
        this.xml = xmlLoader.loadXml(xmlUrl);
    }

    public List<String> getTableNames() {
        return List.of("currencies", "categories", "offers");
    }

    public List<String> getColumnNames(String tableName) {

        GPathResult shop = (GPathResult) xml.getProperty("shop");
        if (shop == null) return Collections.emptyList();

        GPathResult table = (GPathResult) shop.getProperty(tableName);
        if (table == null || table.children().isEmpty()) return Collections.emptyList();

        Set<String> columns = new LinkedHashSet<>();

        Iterator<?> parentIterator = table.nodeIterator();
        if (!parentIterator.hasNext()) return Collections.emptyList();

        Node parentNode = (Node) parentIterator.next();

        Iterator<?> rowIterator = parentNode.children().iterator();
        if (!rowIterator.hasNext()) return Collections.emptyList();

        Node row = (Node) rowIterator.next();

        Map<?, ?> attrs = row.attributes();
        for (Object key : attrs.keySet()) {
            columns.add(key.toString());
        }

        for (Object child : row.children()) {
            if (child instanceof Node nc) {
                columns.add(nc.name());
            }
        }

        if (!row.text().trim().isEmpty()) {
            columns.add("name");
        }

        return new ArrayList<>(columns);
    }

    public boolean isColumnId(String tableName, String columnName) {
        return "offers".equals(tableName) && "vendorCode".equals(columnName);
    }

    public String getTableDDL(String tableName) {
        List<String> columns = getColumnNames(tableName);

        if (columns.isEmpty()) {
            return "-- Table " + tableName + " has no columns --";
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");

        for (String column : columns) {
            ddl.append("  ").append(column).append(" TEXT");
            if (isColumnId(tableName, column)) {
                ddl.append(" PRIMARY KEY");
            }
            ddl.append(",\n");
        }

        ddl.setLength(ddl.length() - 2);
        ddl.append("\n);");

        return ddl.toString();
    }

    public void update() throws SQLException {
        for (String table : getTableNames()) {
            update(table);
        }
    }

    public void update(String tableName) throws SQLException {
        ensureTableStructure(tableName);
        upsertData(tableName);
    }

    private void ensureTableStructure(String tableName) throws SQLException {

        Set<String> expectedColumns = getColumnNames(tableName).stream()
                .map(String::trim)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        Set<String> actualColumns = new HashSet<>();

        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(null, "public", tableName, null)) {
            while (rs.next()) {
                actualColumns.add(
                        rs.getString("COLUMN_NAME").trim().toLowerCase()
                );
            }
        }

        if (actualColumns.isEmpty()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(getTableDDL(tableName));
            }
            return;
        }

        Set<String> missingColumns = new HashSet<>(expectedColumns);
        missingColumns.removeAll(actualColumns);

        if (!missingColumns.isEmpty()) {
            throw new IllegalStateException(
                    "Структура таблицы " + tableName + " изменилась"
            );
        }

        Set<String> extraColumns = new HashSet<>(actualColumns);
        extraColumns.removeAll(expectedColumns);

        if (!extraColumns.isEmpty()) {
            System.out.println("В таблице " + tableName +
                    " есть лишние колонки: " + extraColumns);
        }
    }

    public String getDDLChange(String tableName) throws SQLException {
        List<String> expected = getColumnNames(tableName);
        Set<String> actual = new HashSet<>();

        ResultSet rs = connection.getMetaData()
                .getColumns(null, null, tableName, null);

        while (rs.next()) {
            actual.add(rs.getString("COLUMN_NAME"));
        }
        rs.close();

        StringBuilder ddl = new StringBuilder();
        for (String col : expected) {
            if (!actual.contains(col)) {
                ddl.append("ALTER TABLE ")
                        .append(tableName)
                        .append(" ADD COLUMN ")
                        .append(col)
                        .append(" TEXT;\n");
            }
        }
        return ddl.length() == 0 ? null : ddl.toString();
    }

    private void upsertData(String tableName) throws SQLException {
        GPathResult rows = (GPathResult) xml.getProperty(tableName);

        for (Object rowObj : rows) {
            GPathResult row = (GPathResult) rowObj;

            List<String> columns = new ArrayList<>();
            List<String> values = new ArrayList<>();

            for (Object fieldObj : row.children()) {
                GPathResult field = (GPathResult) fieldObj;
                columns.add(field.name());
                values.add(field.text());
            }

            String idColumn = columns.stream()
                    .filter(c -> isColumnId(tableName, c))
                    .findFirst()
                    .orElse(null);

            String placeholders = String.join(
                    ",", Collections.nCopies(columns.size(), "?")
            );

            String updates = columns.stream()
                    .filter(c -> !c.equals(idColumn))
                    .map(c -> c + "=EXCLUDED." + c)
                    .reduce((a, b) -> a + "," + b)
                    .orElse("");

            String sql =
                    "INSERT INTO " + tableName +
                            " (" + String.join(",", columns) + ") " +
                            "VALUES (" + placeholders + ") " +
                            "ON CONFLICT (" + idColumn + ") " +
                            "DO UPDATE SET " + updates;

            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int i = 0; i < values.size(); i++) {
                    ps.setString(i + 1, values.get(i));
                }
                ps.executeUpdate();
            }
        }
    }
}
