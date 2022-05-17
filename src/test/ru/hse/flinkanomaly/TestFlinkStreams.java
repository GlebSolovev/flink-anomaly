package ru.hse.flinkanomaly;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

public class TestFlinkStreams {

    private StreamExecutionEnvironment env;

    private void applyEnvironmentSettings() {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(1);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.seconds(1)));
    }

    @BeforeEach
    void setUpEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        applyEnvironmentSettings();
    }

    @Test
    void testPrintMapUppercase() throws Exception {
        DataStream<String> stream = env.fromElements("Conquer", "Flink", "!");
        SingleOutputStreamOperator<String> upperCase = stream.map(String::toUpperCase);
        upperCase.print();
        env.execute();
    }

    @Test
    void testPrintWindowed() throws Exception {
        SingleOutputStreamOperator<Tuple2<Integer, Long>> windowed
                = env.fromElements(
                        new Tuple2<>(15, ZonedDateTime.now().plusMinutes(2).toInstant().getEpochSecond()),
                        new Tuple2<>(17, ZonedDateTime.now().plusMinutes(25).plusSeconds(1).toInstant().getEpochSecond()),
                        new Tuple2<>(16, ZonedDateTime.now().plusMinutes(25).toInstant().getEpochSecond()))
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                                new BoundedOutOfOrdernessTimestampExtractor<>(Time.seconds(20)) {
                                    @Override
                                    public long extractTimestamp(Tuple2<Integer, Long> element) {
                                        return element.f1 * 1000;
                                    }
                                }));
        SingleOutputStreamOperator<Tuple2<Integer, Long>> reduced = windowed
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .maxBy(0, true);
        reduced.print();
        env.execute();
    }

}
