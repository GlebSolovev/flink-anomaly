package ru.hse.flinkanomaly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("SameParameterValue")
public class TestFlinkStatefulStreamsRecover {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";

    private StreamExecutionEnvironment env;

    private static final boolean printLogs = false;
    private static final int parallelism = 2;

    private void setParallelism(int parallelism) {
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);
    }

    private void applyEnvironmentSettings() {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        setParallelism(parallelism);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(1)));
    }

    @BeforeEach
    void setUpEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        applyEnvironmentSettings();
    }

    private <T> void executeEnvAndCollectResult(DataStream<T> data) throws Exception {
        env.execute();
        System.out.println("FILTER NAMES: " + UnstableData.validatedNamesLog.get());
    }

    @Test
    void testFewSparseStatefulFails() throws Exception {
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 0, 2000),
                new UnstableData("b", 1, 2000),
                new UnstableData("c", 1, 2000),
                new UnstableData("d", 1, 2000));

        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        KeyedStream<UnstableData, String> keyedData = processedData.keyBy((d) -> "");
        var concatenatedStream = keyedData.flatMap(new StatefulConcatenateFlatMap());

        executeEnvAndCollectResult(concatenatedStream);
    }

    @Test
    void testManyFrequentStatefulFails() throws Exception {
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 0, 500),
                new UnstableData("b", 0, 500),
                new UnstableData("c", 0, 500),
                new UnstableData("d", 1, 500),
                new UnstableData("e", 1, 500),
                new UnstableData("f", 1, 500),
                new UnstableData("g", 1, 500),
                new UnstableData("h", 1, 500)
        );

        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        KeyedStream<UnstableData, String> keyedData = processedData.keyBy((unstableData) -> "");
        var concatenatedStream = keyedData.flatMap(new StatefulConcatenateFlatMap());

        executeEnvAndCollectResult(concatenatedStream);
    }

    public static class StatefulConcatenateFlatMap extends RichFlatMapFunction<UnstableData, String> {

        private transient ValueState<String> concatResult;

        @Override
        public void flatMap(UnstableData value, Collector<String> out) throws Exception {
            String currentResult = concatResult.value();
            if (currentResult == null) {
                currentResult = "";
            }
            currentResult += value.getName();
            concatResult.update(currentResult);

            System.out.println("CURRENT CONCAT RESULT: " + currentResult);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("concatenation result", TypeInformation.of(new TypeHint<>() {
                    }));
            concatResult = getRuntimeContext().getState(descriptor);
        }
    }

    public static class UnstableData {
        public final String name;
        public final int failureTimes;
        public final long waitMillis;

        // must be static otherwise each recover it will be recovered to initial value
        public static final Map<String, Integer> alreadyFailed = new ConcurrentHashMap<>();

        // accumulate logs
        public static final AtomicReference<String> validatedNamesLog = new AtomicReference<>("");

        public static final boolean printLogs = TestFlinkStatefulStreamsRecover.printLogs;

        private void log(String message) {
            if (printLogs) System.out.println(message);
        }

        public String getName() {
            return name;
        }

        public UnstableData(String name, int failureTimes, long waitMillis) {
            this.name = name;
            this.failureTimes = failureTimes;
            this.waitMillis = waitMillis;
            alreadyFailed.put(name, 0);
        }

        public boolean waitValidateOrFail() throws InterruptedException {
            log("COMPUTE " + name + ": current failures = " + alreadyFailed);
            int thisAlreadyFailed = alreadyFailed.get(name);
            if (thisAlreadyFailed < failureTimes) {
                alreadyFailed.put(name, thisAlreadyFailed + 1);
                log("Data " + name + " failed");
                validatedNamesLog.getAndUpdate(log -> log += ANSI_RED + name + ANSI_RESET);
                throw new UnstableDataFailedException();
            }
            validatedNamesLog.getAndUpdate(log -> log += name);
            TimeUnit.MILLISECONDS.sleep(waitMillis);
            return true;
        }

        public String toString() {
            return "OUT " + name + "\n";
        }
    }

    public static class UnstableDataFailedException extends RuntimeException {
    }

}
