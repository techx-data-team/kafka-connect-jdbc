package io.confluent.connect.jdbc;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Arrays;

import org.junit.Test;

import io.confluent.connect.jdbc.dialect.SqlServerDatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableDefinitionBuilder;
import io.confluent.connect.jdbc.util.TableId;

public class JdbcTestSqlDialect {
    protected JdbcSourceConnectorConfig sourceConfigWithUrl(
            String url) {
        Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
        connProps.put("quote.sql.identifiers", QuoteMethod.ALWAYS.toString());
        return new JdbcSourceConnectorConfig(connProps);
    }

    @Test
    public void testUpsert() {
        TableId tableId = new TableId(null, null, "myTable");;
        TableDefinitionBuilder builder = new TableDefinitionBuilder().withTable(tableId.toString());
        builder.withColumn("id1").type("int", JDBCType.INTEGER, Integer.class);
        builder.withColumn("id2").type("int", JDBCType.INTEGER, Integer.class);
        builder.withColumn("columnA").type("varchar", JDBCType.VARCHAR, String.class).displaySize(30);
        builder.withColumn("columnB").type("varchar", JDBCType.VARCHAR, String.class).displaySize(30);
        builder.withColumn("columnC").type("numeric", JDBCType.VARCHAR, String.class).precision(5).scale(2);
        builder.withColumn("columnD").type("decimal", JDBCType.VARCHAR, String.class).precision(5).scale(1);
        TableDefinition tableDefn = builder.build();
        SqlServerDatabaseDialect dialect = new SqlServerDatabaseDialect(sourceConfigWithUrl("jdbc:postgresql://something"));
        ColumnId columnPK1 = new ColumnId(tableId, "id1");
        ColumnId columnPK2 = new ColumnId(tableId, "id2");
        ColumnId columnA = new ColumnId(tableId, "columnA");
        ColumnId columnB = new ColumnId(tableId, "columnB");
        ColumnId columnC = new ColumnId(tableId, "columnC");
        ColumnId columnD = new ColumnId(tableId, "columnD");
        List<ColumnId> pkColumns = Arrays.asList(columnPK1, columnPK2);
        List<ColumnId> columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);
        String query = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD, tableDefn);
        System.out.println(query);
    }
}
