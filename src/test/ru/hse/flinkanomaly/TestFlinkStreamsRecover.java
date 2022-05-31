package ru.hse.flinkanomaly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestFlinkStreamsRecover {

    private StreamExecutionEnvironment env;

    private void applyEnvironmentSettings() {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(1);
        env.setMaxParallelism(1);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(1)));
    }

    @BeforeEach
    void setUpEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        applyEnvironmentSettings();
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

        processedData.print();
        env.execute();
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

        processedData.print();
        env.execute();
    }

    @Test
    void testMultipleStreams() throws Exception { // probably, is possible to catch anomaly!
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

        processedData.print();
        env.execute();
    }

    public static class UnstableData {
        public final String name;
        public final int failureTimes;
        public final long waitMillis;

        // must be static otherwise each recover it will be recovered to initial value
        public static Map<String, Integer> alreadyFailed = new HashMap<>();

        public UnstableData(String name, int failureTimes, long waitMillis) {
            this.name = name;
            this.failureTimes = failureTimes;
            this.waitMillis = waitMillis;
            alreadyFailed.put(name, 0);
        }

        public boolean waitValidateOrFail() throws InterruptedException {
            System.out.println("COMPUTE " + name + ": current failures = " + alreadyFailed);
            int thisAlreadyFailed = alreadyFailed.get(name);
            if (thisAlreadyFailed < failureTimes) {
                System.out.println(thisAlreadyFailed);
                alreadyFailed.put(name, thisAlreadyFailed + 1);
                System.out.println("Data " + name + " failed");
                throw new UnstableDataFailedException();
            }
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
