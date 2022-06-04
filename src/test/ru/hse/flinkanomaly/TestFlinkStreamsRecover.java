package ru.hse.flinkanomaly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("SameParameterValue")
public class TestFlinkStreamsRecover {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";

    private StreamExecutionEnvironment env;

    private static final boolean printLogs = false;
    private static final int parallelism = 4;

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

    private void executeEnvAndCollectResult(DataStream<UnstableData> data) throws Exception {
        data.addSink(new ConcatenateSink());
        if (printLogs) data.print();

        env.execute();

        System.out.println("FILTER NAMES: " + UnstableData.validatedNamesLog.get());
        System.out.println("RESULT NAMES: " + ConcatenateSink.result.get() + "\n");
    }

    @Test
    void testOneFail() throws Exception {
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 0, 1500),
                new UnstableData("b", 0, 1500),
                new UnstableData("c", 1, 1500),
                new UnstableData("d", 0, 1500));
        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        executeEnvAndCollectResult(processedData);
    }

    @Test
    void testMultipleFails() throws Exception {
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 1, 1500),
                new UnstableData("b", 1, 1500),
                new UnstableData("c", 1, 1500),
                new UnstableData("d", 1, 1500));
        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        executeEnvAndCollectResult(processedData);
    }

    @Test
    void testMultipleStreams() throws Exception {
        DataStream<UnstableData> stream1 = env.fromElements(
                new UnstableData("a", 1, 100),
                new UnstableData("b", 1, 100),
                new UnstableData("c", 1, 100));
        DataStream<UnstableData> stream2 = env.fromElements(
                new UnstableData("d", 1, 100),
                new UnstableData("e", 1, 1500));

        var unionStream = stream1.union(stream2);
        DataStream<UnstableData> processedData = unionStream.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        executeEnvAndCollectResult(processedData);
    }

    public static class ConcatenateSink extends RichSinkFunction<UnstableData> {
        public static final AtomicReference<String> result = new AtomicReference<>("");

        @Override
        public void invoke(UnstableData value, Context context) throws Exception {
            super.invoke(value, context);
            result.getAndUpdate(res -> res += value.name);
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

        public static final boolean printLogs = TestFlinkStreamsRecover.printLogs;

        private void log(String message) {
            if (printLogs) System.out.println(message);
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
