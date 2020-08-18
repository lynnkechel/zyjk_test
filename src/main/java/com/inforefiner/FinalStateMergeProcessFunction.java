//package com.inforefiner;
//
////import com.inforefiner.streaming.runner.flink.utils.ClassUtil;
////import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.state.MapState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.metrics.MetricGroup;
//import org.apache.flink.streaming.api.TimerService;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//
//import java.util.*;
//
///**
// * Created by P0007 on 2020/4/23.
// * 最终状态或者缓存数据waitSeconds秒数触发据合并
// */
////@Slf4j
//public class FinalStateMergeProcessFunction extends KeyedProcessFunction<Tuple, Row, Row> {
//
//    private final RowTypeInfo rowTypeInfo;
//
//    private final long waitSeconds;
//
//    private final Map<Integer, List> finalStateColIndexes;
//
//    private final boolean discard;
//
//    private final OutputTag<Row> outputTag;
//
//    private transient ValueState<Long> registerTimeState;
//
//    private transient MapState<Long, Row> mapState;
//
//    private boolean init = true;
//
//    private transient long mergedCnt = 0L;
//
//    private transient long processedCnt = 0L;
//
//    private transient long finalStateCnt = 0L;
//
//    private transient long noFinalStateCnt = 0L;
//
//    private transient long mapStateSize = 0L;
//
//    private transient long recoverMapStateSize = 0L;
//
//    /**
//     * finalStateSettings 支持多字段多值设置 colA:state1,state2;colB:state4,state5
//     *
//     * @param rowTypeInfo
//     * @param waitSeconds
//     * @param discard
//     * @param discardSideOutputTag
//     * @param finalStateSettings
//     * @param columnSeparator     字段与字段分隔符，默认是分号
//     * @param keyValueSeparator   字段与值分隔符，默认是冒号
//     * @param finalStateSeparator 值与值分隔符，默认是逗号
//     */
//    public FinalStateMergeProcessFunction(RowTypeInfo rowTypeInfo, long waitSeconds, boolean discard,
//                                          OutputTag discardSideOutputTag, String finalStateSettings, String columnSeparator,
//                                          String keyValueSeparator, String finalStateSeparator) {
//        this.rowTypeInfo = rowTypeInfo;
//        this.waitSeconds = waitSeconds;
//        this.finalStateColIndexes = parserFinalStateSettings(rowTypeInfo, finalStateSettings,
//                columnSeparator, keyValueSeparator, finalStateSeparator);
//        this.discard = discard;
//        this.outputTag = discardSideOutputTag;
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        TypeInformation<Long> keyInfo = TypeInformation.of(Long.class);
//        TypeInformation<Row> valueInfo = TypeInformation.of(Row.class);
//        MapStateDescriptor stateDescriptor = new MapStateDescriptor("buffer", keyInfo, valueInfo);
//        this.mapState = getRuntimeContext().getMapState(stateDescriptor);
//
//        TypeInformation<Long> registerTimeStateInfo = TypeInformation.of(Long.class);
//        ValueStateDescriptor<Long> registerTimeStateDesc = new ValueStateDescriptor<>(
//                "registerTime", registerTimeStateInfo);
//        this.registerTimeState = getRuntimeContext().getState(registerTimeStateDesc);
//
//        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("custom_group");
//
//        //metrics
//        metricGroup.gauge("mergedCnt", () -> mergedCnt);
//
//        metricGroup.gauge("processedCnt", () -> processedCnt);
//
//        metricGroup.gauge("finalStateCnt", () -> finalStateCnt);
//
//        metricGroup.gauge("noFinalStateCnt", () -> noFinalStateCnt);
//
//        metricGroup.gauge("mapStateSize", () -> mapStateSize);
//
//        metricGroup.gauge("recoverMapStateSize", () -> recoverMapStateSize);
//    }
//
//    @Override
//    public void processElement(Row newRow, Context ctx, Collector<Row> out) throws Exception {
//
//        /**
//         * 由于state无法在{@link #open(Configuration)}中无法操作state,否则抛出异常：
//         * NullPointerException: No key set. This method should not be called outside of a keyed context.
//         */
//        if (init) {
//            Iterator<Long> keyIterator = this.mapState.keys().iterator();
//            while (keyIterator.hasNext()) {
//                keyIterator.next();
//                this.recoverMapStateSize++;
//            }
//            this.mapStateSize = this.recoverMapStateSize;
//            log.info("recover map state size: {}", this.recoverMapStateSize);
//            init = false;
//        }
//
//        TimerService timerService = ctx.timerService();
//        Long newEventTime = ctx.timestamp();
//        /* 没有指定eventTime或者使用ProcessTime */
//        if (newEventTime == null) {
//            throw new RuntimeException("Please assign timestamps,watermarks.");
//        }
//        Long registerTime = registerTimeState.value();
//        if (registerTime != null) {
//            //merge all rows
//            if (registerTime < newEventTime) {
//                Row row = mapState.get(registerTime);
//                if (row != null) {
//                    noFinalStateCnt++;
//                    out.collect(row);
//                }
//                mapState.clear();
//                registerTimeState.clear();
//                log.debug("registerTime < newEventTime: {} < {}, clear and collect row {}",
//                        registerTime, newEventTime, row);
//                processNoBufferRow(newRow, timerService, newEventTime, ctx.getCurrentKey(), out);
//            } else {
//                long rowEventTime = registerTime - 1000 * waitSeconds;
//                boolean late = newEventTime < rowEventTime;
//                Row row = mapState.get(registerTime);
//                log.debug("Merging row :\n{}\n{}", newRow, row);
//                if (row == null || newRow == null) {
//                    log.error("row is empty. registerTime is {}.\n{}\n{}", registerTime, row, newRow);
//                    return;
//                }
//                if (newRow.getArity() != row.getArity()) {
//                    throw new RuntimeException("Row arity not equal");
//                } else {
//                    for (int i = 0; i < newRow.getArity(); i++) {
//                        Object valueFieldV = newRow.getField(i);
//                        Object rowFieldV = row.getField(i);
//                        String type = rowTypeInfo.getTypeAt(i).toString();
//                        if (late) {
//                            row.setField(i, ClassUtil.isEmpty(rowFieldV, type) ? valueFieldV : rowFieldV);
//                        } else {
//                            row.setField(i, ClassUtil.isEmpty(valueFieldV, type) ? rowFieldV : valueFieldV);
//                        }
//                    }
//                }
//                mergedCnt++;
//                log.debug("Merged result {}", row);
//
//                /*
//                 * 如果是终态则out.collect，否则更新mapState
//                 * */
//                if (isFinalState(newRow)) {
//                    log.debug("Clear state of key @ {} and registerTime {}, collect row {}", ctx.getCurrentKey(),
//                            registerTime, row);
//                    out.collect(row);
//                    mapState.clear();
//                    registerTimeState.clear();
//                    timerService.deleteEventTimeTimer(registerTime);
//                    mapStateSize--;
//                    finalStateCnt++;
//                } else {
//                    mapState.put(registerTime, row);
//                }
//            }
//        } else {
//            log.debug("No such key {} exists in the buffer", ctx.getCurrentKey());
//            processNoBufferRow(newRow, timerService, newEventTime, ctx.getCurrentKey(), out);
//        }
//        processedCnt++;
//    }
//
//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {
//        TimerService timerService = ctx.timerService();
//        Row row = mapState.get(timestamp);
//        log.debug("On Timer @ {}, collect row {}", timestamp, row);
//        /*
//         * 超时数据
//         * */
//        if (!discard && row != null) {
//            ctx.output(outputTag, row);
//        }
//        mapState.clear();
//        registerTimeState.clear();
//        noFinalStateCnt++;
//        mapStateSize--;
//        timerService.deleteEventTimeTimer(timestamp);
//    }
//
//    /**
//     * 解析终态参数设置
//     *
//     * @param rowTypeInfo
//     * @param finalStateSettings
//     * @param columnSeparator
//     * @param keyValueSeparator
//     * @param finalStateSeparator
//     * @return 终态
//     */
//    private Map<Integer, List> parserFinalStateSettings(RowTypeInfo rowTypeInfo, String finalStateSettings, String columnSeparator,
//                                                        String keyValueSeparator, String finalStateSeparator) {
//
//        Map<Integer, List> finalStateMapping = new HashMap<>();
//        List finalStateList = new ArrayList<>();
//        try {
//            String[] finalStateColAndValues = finalStateSettings.split(columnSeparator);
//            for (String finalStateColAndValue : finalStateColAndValues) {
//                String[] keyAndValue = finalStateColAndValue.split(keyValueSeparator);
//                String finalStateCol = keyAndValue[0];
//                String finalStateValue = keyAndValue[1];
//                int fieldIndex = rowTypeInfo.getFieldIndex(finalStateCol);
//                if (fieldIndex == -1) {
//                    throw new RuntimeException("Final state column "
//                            + finalStateCol + " not exist in " + finalStateSettings);
//                } else {
//                    String type = rowTypeInfo.getTypeAt(finalStateCol).toString();
//                    String[] finalStates = finalStateValue.split(finalStateSeparator);
//                    for (String finalState : finalStates) {
//                        Object convertdValue = ClassUtil.convert(finalState, type);
//                        finalStateList.add(convertdValue);
//                    }
//                }
//                finalStateMapping.put(fieldIndex, finalStateList);
//            }
//            log.debug("Parsed final state value is {}, rowTypeInfo is {}", finalStateMapping, rowTypeInfo);
//            return finalStateMapping;
//        } catch (Exception e) {
//            throw new RuntimeException("Parser final state values throw exception", e);
//        }
//    }
//
//    /**
//     * 处理新到的数据，注册Timer等待触发
//     *
//     * @param newRow        新到的数据
//     * @param timerService  timer服务
//     * @param eventTime     事件时间
//     * @param keyFieldValue key字段的值
//     * @throws Exception Thrown if the system cannot access the state.
//     */
//    private void processNoBufferRow(Row newRow, TimerService timerService, Long eventTime,
//                                    Object keyFieldValue, Collector<Row> out) throws Exception {
//
//        /*
//         * 如果是终态则out.collect，不再处理
//         * */
//        boolean isFinalState = isFinalState(newRow);
//        if (isFinalState) {
//            finalStateCnt++;
//            out.collect(newRow);
//            return;
//        }
//
//        /*
//         * 注册waitSeconds秒后的Timer，以触发向下发送数据
//         * */
//        Long registerTime = eventTime + 1000 * waitSeconds;
//        mapState.put(registerTime, newRow);
//        mapStateSize++;
//        registerTimeState.update(registerTime);
//        timerService.registerEventTimeTimer(registerTime);
//        log.debug("Register timer @ {}, key is {}", registerTime, keyFieldValue);
//    }
//
//    private boolean isFinalState(Row newRow) {
//        boolean isFinalState = false;
//        for (Integer index : finalStateColIndexes.keySet()) {
//            Object finalState = newRow.getField(index);
//            List finalStates = finalStateColIndexes.get(index);
//            if (finalStates.contains(finalState)) {
//                isFinalState = true;
//                log.debug("On final state @ {}, row {}", finalState, newRow);
//                break;
//            }
//        }
//        return isFinalState;
//    }
//}
