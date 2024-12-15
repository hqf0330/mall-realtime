package org.mason77.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.mason77.base.BaseApp;
import org.mason77.constant.StreamConstant;
import org.mason77.util.FlinkSinkUtil;

public class DwdBaseLogApp extends BaseApp {

    // 注意范型擦除，匿名内部类
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

    public static void main(String[] args) {
        new DwdBaseLogApp().start(10002, 3, "dwd_base_log", StreamConstant.KAFKA_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将字符串转为json，并且脏数据进入测流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });

        jsonObjDS.print("main");
        SideOutputDataStream<String> dirtyDataDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDataDS.print("dirty");

        // 脏数据写入到kafka中
        KafkaSink<String> dirtyDataSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDataDS.sinkTo(dirtyDataSink);

    }
}
