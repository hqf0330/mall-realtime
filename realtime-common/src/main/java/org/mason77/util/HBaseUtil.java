package org.mason77.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Set;

@Slf4j
public class HBaseUtil {

    public static Connection getConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node02,node03,node04");
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void createHBaseTable(Connection conn, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("At least one family!");
            return;
        }

        try (Admin admin = conn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                log.warn(namespace + ":" + tableName + " exists!!!");
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            log.warn(namespace + ":" + tableName + " has been created!!!");

        } catch (IOException e) {
            log.error("create hbase table failed", e);
        }
    }


    public static void dropHBaseTable(Connection conn, String namespace, String table) {
        try (Admin admin = conn.getAdmin()) {
            TableName tableObj = TableName.valueOf(namespace, table);
            if (!admin.tableExists(tableObj)) {
                System.out.println(namespace + ":" + table + " dose not exist!!!!");
                return;
            }
            admin.disableTable(tableObj);
            admin.deleteTable(tableObj);
            System.out.println(namespace + ":" + table + " has been droped!!!!");
        } catch (IOException e) {
            log.error("drop hbase table failed", e);
        }
    }


    public static void putRow(Connection conn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObject) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = conn.getTable(tableNameObj)) {
            // 封装put
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObject.keySet();
            columns.forEach(column -> {
                String value = jsonObject.getString(column);
                if (!Strings.isNullOrEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            });
            table.put(put);
            System.out.println(namespace + ":" + tableName + " has been added!!!!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void delRow(Connection conn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = conn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println(namespace + ":" + tableName + " has been deleted!!!!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
