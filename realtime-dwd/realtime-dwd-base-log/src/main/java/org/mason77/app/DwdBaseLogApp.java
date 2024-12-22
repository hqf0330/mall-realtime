package org.mason77.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
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

import java.util.HashMap;
import java.util.Map;

public class DwdBaseLogApp extends BaseApp {

    // 注意范型擦除，匿名内部类
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<String>("dirtyTag", TypeInformation.of(String.class));
    private static final OutputTag<String> ERR_TAG = new OutputTag<String>("errTag", TypeInformation.of(String.class));
    private static final OutputTag<String> START_TAG = new OutputTag<String>("startTag", TypeInformation.of(String.class));
    private static final OutputTag<String> DISPLAY_TAG = new OutputTag<String>("displayTag", TypeInformation.of(String.class));
    private static final OutputTag<String> ACTION_TAG = new OutputTag<String>("actionTag", TypeInformation.of(String.class));

    private static final String ERR = "err";
    private static final String START = "start";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLogApp().start(10002, 3, "dwd_base_log", StreamConstant.KAFKA_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 将字符串转为json，并且脏数据进入侧流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        // 新老访客的标记修复
        SingleOutputStreamOperator<JSONObject> fixedDS = fixNewUser(jsonObjDS);

        // 日志分流
        Map<String, DataStream<String>> streamMap = splitStream(fixedDS);

        // sink to kafka
        sinkToKafka(streamMap);

    }

    private static void sinkToKafka(Map<String, DataStream<String>> streamMap) {
        streamMap.get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(StreamConstant.TOPIC_DWD_TRAFFIC_PAGE));

        streamMap.get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(StreamConstant.TOPIC_DWD_TRAFFIC_START));

        streamMap.get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(StreamConstant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streamMap.get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(StreamConstant.TOPIC_DWD_TRAFFIC_ACTION));

        streamMap.get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(StreamConstant.TOPIC_DWD_TRAFFIC_ERR));
    }

    private static Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> fixedDS) {
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject errJsonObj = jsonObject.getJSONObject("err");
                if (errJsonObj != null) {
                    context.output(ERR_TAG, jsonObject.toJSONString());
                    jsonObject.remove("err");
                }

                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                if (startJsonObj != null) {
                    context.output(START_TAG, jsonObject.toJSONString());
                } else {
                    JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");

                    JSONArray displayJsonArr = jsonObject.getJSONArray("display");
                    if (displayJsonArr != null && !displayJsonArr.isEmpty()) {
                        for (int i = 0; i < displayJsonArr.size(); i++) {
                            JSONObject displayJsonObj = displayJsonArr.getJSONObject(i);
                            JSONObject newDisplayJsonObj = new JSONObject();
                            newDisplayJsonObj.put("common", commonJsonObj);
                            newDisplayJsonObj.put("page", pageJsonObj);
                            newDisplayJsonObj.put("display", displayJsonObj);
                            newDisplayJsonObj.put("ts", ts);
                            context.output(DISPLAY_TAG, newDisplayJsonObj.toJSONString());
                        }

                        jsonObject.remove("displays");
                    }

                    JSONArray actionArr = jsonObject.getJSONArray("actions");
                    if (actionArr != null && !actionArr.isEmpty()) {
                        for (int i = 0; i < actionArr.size(); i++) {
                            JSONObject actionJsonOb = actionArr.getJSONObject(i);
                            JSONObject newActionJsonObj = new JSONObject();
                            newActionJsonObj.put("common", commonJsonObj);
                            newActionJsonObj.put("page", pageJsonObj);
                            newActionJsonObj.put("action", actionJsonOb);
                            context.output(ACTION_TAG, newActionJsonObj.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                }

                collector.collect(jsonObject.toJSONString());
            }
        });

        // 将流写入到不同的topic中

        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(PAGE, pageDS);
        streamMap.put(ERR, pageDS.getSideOutput(ERR_TAG));
        streamMap.put(START, pageDS.getSideOutput(START_TAG));
        streamMap.put(DISPLAY, pageDS.getSideOutput(DISPLAY_TAG));
        streamMap.put(ACTION, pageDS.getSideOutput(ACTION_TAG));
        return streamMap;
    }

    private static SingleOutputStreamOperator<JSONObject> fixNewUser(SingleOutputStreamOperator<JSONObject> jsonObjDS) {
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
                        if ("1".equals(isNew)) {

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
        return fixedDS;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(DIRTY_TAG, s);
                }
            }
        });

        // jsonObjDS.print("main");
        SideOutputDataStream<String> dirtyDataDS = jsonObjDS.getSideOutput(DIRTY_TAG);

        // 脏数据写入到kafka中
        KafkaSink<String> dirtyDataSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDataDS.sinkTo(dirtyDataSink);
        return jsonObjDS;
    }
}
