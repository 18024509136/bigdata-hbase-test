package com.geek;

import java.util.ArrayList;
import java.util.List;

public class HbaseTest {

    public static void main(String[] args) {
        String namespace = "huangxiaodi";
        String tableName = "student";
        String studentId = "G20210675010604";
        String rowKey = studentId;

        try {
            // 创建命名空间
            HbaseUtil.createNamespace(namespace);
            // 创建表
            HbaseUtil.createTable(namespace, tableName, "info", "score");
            // 插入info列簇数据,studentId作为rowkey
            List<HbaseUtil.ValueWrapper> infoValueWrappers = new ArrayList<>(3);
            infoValueWrappers.add(new HbaseUtil.ValueWrapper("name", "huangxiaodi"));
            infoValueWrappers.add(new HbaseUtil.ValueWrapper("student_id", studentId));
            infoValueWrappers.add(new HbaseUtil.ValueWrapper("class", "5"));
            HbaseUtil.insertMultiColumn("huangxiaodi", "student", rowKey,
                    "info", infoValueWrappers);

            // 插入score列簇数据
            List<HbaseUtil.ValueWrapper> scoreValueWrappers = new ArrayList<>(2);
            scoreValueWrappers.add(new HbaseUtil.ValueWrapper("understanding", "60"));
            scoreValueWrappers.add(new HbaseUtil.ValueWrapper("programming", "60"));
            HbaseUtil.insertMultiColumn(namespace, tableName, rowKey,
                    "score", scoreValueWrappers);

            // 查询rowkey为G20210675010604的所有列数据
            HbaseUtil.getRow(namespace, tableName, rowKey);

            // 删除rowkey为G20210675010604的所有列数据
            //HbaseUtil.deleteRow(namespace, tableName, rowKey);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HbaseUtil.closeResources();
        }
    }
}
