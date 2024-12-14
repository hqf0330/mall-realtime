package org.mason77.function;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.mason77.bean.TableProcessDim;
import org.mason77.constant.StreamConstant;
import org.mason77.util.HBaseUtil;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(conn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim dimTable = value.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        String sinkTable = dimTable.getSinkTable();
        String rowKey = jsonObj.getString(dimTable.getSinkRowKey());

        if ("delete".equals(type)) {
            HBaseUtil.delRow(conn, StreamConstant.HBASE_NAMESPACE, sinkTable, rowKey);
            log.info(sinkTable + ": " + rowKey + " deleted.");
        } else {
            String sinkFamily = dimTable.getSinkFamily();
            HBaseUtil.putRow(conn, StreamConstant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
            log.info(sinkTable + ": " + rowKey + " added.");
        }
    }
}
