package org.mason77.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
import org.mason77.util.DateFormatUtil;
import org.mason77.util.FlinkSinkUtil;

public class DwdBaseLogApp extends BaseApp {

    // 注意范型擦除，匿名内部类
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
    };

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

        // jsonObjDS.print("main");
        SideOutputDataStream<String> dirtyDataDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDataDS.print("dirty");

        // 脏数据写入到kafka中
        KafkaSink<String> dirtyDataSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDataDS.sinkTo(dirtyDataSink);

        // 新老访客的标记修复
        SingleOutputStreamOperator<JSONObject> fixedDS = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDate", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取新否新用户
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        // 获取时间状态
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        if("1".equals(isNew)) {

                            if (StringUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }
                        }
                        return jsonObject;
                    }
                });

        fixedDS.print("fixed");

    }
}
