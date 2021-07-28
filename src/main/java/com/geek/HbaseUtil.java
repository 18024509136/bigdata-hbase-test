package com.geek;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hbase工具类
 *
 * @author huangxiaodi
 * @since 2021-07-28
 */
@Slf4j
public class HbaseUtil {

    private static Configuration conf;
    private static Connection connection;
    private static HBaseAdmin admin;

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, "jikehadoop01:2181,jikehadoop02:2181,jikehadoop03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            log.error("Hbase客户端资源初始化失败！", e);
        }
    }

    /**
     * 关闭资源
     */
    public static void closeResources() {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            log.error("关闭admin资源失败！", e);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            log.error("关闭连接失败！", e);
        }
    }

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间名称
     * @throws Exception
     */
    public static void createNamespace(String namespace) throws Exception {
        try {
            if (namespaceExists(namespace)) {
                return;
            }

            NamespaceDescriptor nsd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nsd);
            log.info("创建命名空间{}成功！", namespace);
        } catch (IOException e) {
            log.error("创建命名空间失败！", e);
            throw e;
        }
    }

    /**
     * 判断命名空间是否存在
     *
     * @param namespace 命名空间名称
     * @return
     * @throws Exception
     */
    public static boolean namespaceExists(String namespace) throws Exception {
        try {
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor nsd : namespaceDescriptors) {
                if (nsd.getName().equals(namespace)) {
                    log.info("命名空间{}存在", namespace);
                    return true;
                }
            }
            log.info("命名空间{}不存在", namespace);
            return false;
        } catch (IOException e) {
            log.error("命名空间检测异常！", e);
            throw e;
        }
    }

    /**
     * 创建表
     *
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param families  列簇列表
     * @throws Exception
     */
    public static void createTable(String namespace, String tableName, String... families) throws Exception {
        try {
            TableName tName = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                admin.deleteTable(tName);

                log.info("表{}存在，删除表成功！", tableName);
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tName);
            for (String family : families) {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
            }
            admin.createTable(builder.build());
            log.info("创建表{}成功！", tableName);
        } catch (IOException e) {
            log.error("创建表" + tableName + "出现异常！", e);
            throw e;
        }
    }

    /**
     * 插入单个列簇多列值
     *
     * @param namespace     命名空间名称
     * @param tableName     表名
     * @param rowKey        行键
     * @param family        列簇名称
     * @param valueWrappers 列值封装对象
     * @throws Exception
     */
    public static void insertMultiColumn(String namespace, String tableName, String rowKey, String family, List<ValueWrapper> valueWrappers)
            throws Exception {
        try {
            TableName tName = TableName.valueOf(namespace, tableName);
            Table table = connection.getTable(tName);

            List<Put> puts = new ArrayList<>();
            for (ValueWrapper valueWrapper : valueWrappers) {
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(valueWrapper.getQualifier()), Bytes.toBytes(valueWrapper.getValue()));
                puts.add(put);
            }
            table.put(puts);
            log.info("插入表{}的{}列簇数据成功！", tableName, family);
        } catch (IOException e) {
            log.error("插入数据失败！", e);
            throw e;
        }
    }

    /**
     * 根据单个rowKey查询行数据
     *
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param rowKey    行键
     * @throws Exception
     */
    public static void getRow(String namespace, String tableName, String rowKey) throws Exception {
        try {
            TableName tName = TableName.valueOf(namespace, tableName);
            Table table = connection.getTable(tName);

            Get get = new Get(Bytes.toBytes(rowKey));
            Result rs = table.get(get);
            Cell[] cells = rs.rawCells();

            log.info("返回rowKey为{}的行数据:", rowKey);
            for (Cell c : cells) {
                String familyName = Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
                String columnName = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength());
                String columnValue = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());

                log.info("列簇{}，列名{}，列值{}", familyName, columnName, columnValue);
            }
        } catch (IOException e) {
            log.error("查询异常", e);
            throw e;
        }

    }

    /**
     * 根据rowKey删除整行数据
     *
     * @param namespace 命名空间名称
     * @param tableName 表名
     * @param rowKey    行键
     * @throws Exception
     */
    public static void deleteRow(String namespace, String tableName, String rowKey) throws Exception {
        try {
            TableName tName = TableName.valueOf(namespace, tableName);
            Table table = connection.getTable(tName);

            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);

            log.info("删除rowKey为{}的行成功！", rowKey);
        } catch (IOException e) {
            log.info("删除行失败！", e);
            throw e;
        }
    }

    @Data
    public static class ValueWrapper {

        /**
         * 列标识
         */
        private String qualifier;

        /**
         * 列值
         */
        private String value;

        public ValueWrapper(String qualifier, String value) {
            this.qualifier = qualifier;
            this.value = value;
        }
    }
}
