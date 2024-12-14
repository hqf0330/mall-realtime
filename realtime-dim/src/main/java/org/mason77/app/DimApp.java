package org.mason77.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.mason77.base.BaseApp;
import org.mason77.bean.TableProcessDim;
import org.mason77.constant.StreamConstant;
import org.mason77.function.HBaseSinkFunction;
import org.mason77.util.FlinkSourceUtil;
import org.mason77.util.HBaseUtil;
import org.mason77.util.JDBCUtil;

import java.util.*;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 3, "dim_app", StreamConstant.KAFKA_TOPIC);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = stream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context,
                                       Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String db = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                String data = jsonObject.getString("data");
                if ("gmall".equals(db)
                        && ("insert".equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type)
                        && data != null
                        && data.length() > 2)) {
                    out.collect(jsonObject);
                }
            }
        });

        //todo 4. 使用flink cdc读取配置表的配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(StreamConstant.MYSQL_DB, StreamConstant.MYSQL_TABLE);
        DataStreamSource<String> mysqlDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySql Source")
                .setParallelism(1);

        //todo 5. 配置信息转为实体
        SingleOutputStreamOperator<TableProcessDim> dimTableDS = mysqlDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String op = jsonObject.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    // before中删除配置
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    // after中获取最新
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);

        //todo 6. 根据配置信息对hbase进行操作
        dimTableDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getConnection();
            }

            @Override
            public TableProcessDim map(TableProcessDim value) throws Exception {
                String op = value.getOp();
                String sinkTable = value.getSinkTable();
                String[] families = value.getSinkFamily().split(",");

                if ("d".equals(op)) {
                    HBaseUtil.dropHBaseTable(hbaseConn, StreamConstant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    HBaseUtil.createHBaseTable(hbaseConn, StreamConstant.HBASE_NAMESPACE, sinkTable, families);
                } else {
                    HBaseUtil.dropHBaseTable(hbaseConn, StreamConstant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn, StreamConstant.HBASE_NAMESPACE, sinkTable, families);
                }

                return value;
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(hbaseConn);
            }

        }).setParallelism(1);

        //todo 7. 将配置流进行广播
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDimTableDS = dimTableDS.broadcast(mapStateDescriptor);

        //todo 8. 主流和广播流进行关联
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedDS = jsonObjectDS.connect(broadcastDimTableDS);

        //todo 9. 处理关联后的数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> resultDS = connectedDS.process(new BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {

            private final Map<String, TableProcessDim> configMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                java.sql.Connection conn = JDBCUtil.getConnection(
                        StreamConstant.MYSQL_DRIVER
                        , StreamConstant.MYSQL_URL
                        , StreamConstant.MYSQL_USER_NAME
                        , StreamConstant.MYSQL_PASSWORD
                );

                List<TableProcessDim> tableProcessDims = JDBCUtil.queryList(
                        conn
                        , "select * from gmall_config.table_process_dim"
                        , TableProcessDim.class
                        , true
                );

                tableProcessDims.forEach(dim -> {
                            String key = dim.getSourceTable();
                            configMap.put(key, dim);
                        }
                );

                JDBCUtil.closeConnection(conn);
            }

            @Override
            public void processBroadcastElement(TableProcessDim tp,
                                                BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx,
                                                Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

                String op = tp.getOp();
                BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String sourceTable = tp.getSourceTable();
                if ("d".equals(op)) {
                    broadcastState.remove(sourceTable);
                    configMap.remove(sourceTable);
                } else {
                    broadcastState.put(sourceTable, tp);
                    // 保证数据的一致性
                    configMap.put(sourceTable, tp);
                }
            }

            @Override
            public void processElement(JSONObject jsonObject,
                                       BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx,
                                       Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                String table = jsonObject.getString("table");
                ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDim tableProcessDim = null;
                if ((tableProcessDim = broadcastState.get(table)) != null
                        || (tableProcessDim = configMap.get(table)) != null) {
                    JSONObject dataJsonObject = jsonObject.getJSONObject("data");

                    // 过滤掉非必要的字段
                    String sinkColumns = tableProcessDim.getSinkColumns();
                    deleteNotNeedColumns(dataJsonObject, sinkColumns);

                    // 加上数据修改类型
                    String type = jsonObject.getString("type");
                    dataJsonObject.put("type", type);
                    out.collect(Tuple2.of(dataJsonObject, tableProcessDim));
                }
            }

        });

        resultDS.print();
        resultDS.addSink(new HBaseSinkFunction());
    }

    private static void deleteNotNeedColumns(JSONObject jsonObject, String sinkColumns) {
        List<String> columList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = jsonObject.entrySet();
        entrySet.removeIf(e -> !columList.contains(e.getKey()));
    }

}