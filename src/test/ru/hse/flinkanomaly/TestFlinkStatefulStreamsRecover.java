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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("SameParameterValue")
public class TestFlinkStatefulStreamsRecover {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_BLUE = "\u001B[34m";

    private StreamExecutionEnvironment env;

    private static final boolean printLogs = true;
    private static final long checkpointingIntervalMillis = 1000;

    private void setParallelism(int parallelism) {
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);
    }

    private void applyDefaultEnvironmentSettings() {
        env.enableCheckpointing(checkpointingIntervalMillis, CheckpointingMode.AT_LEAST_ONCE);
        setParallelism(2);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(1)));
    }

    @BeforeEach
    void setUpEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        applyDefaultEnvironmentSettings();
        UnstableData.resetAlreadyFailedState(); // UnstableData implementation must be reset on every test run
    }

    private <T> void executeEnvAndCollectResult(DataStream<T> data) throws Exception {
        data.print();
        env.execute();
        if (printLogs) {
            System.out.println("FILTER NAMES: " + UnstableData.validatedNamesLog.get());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testFewSparseStatefulFails(int parallelism) throws Exception {
        setParallelism(parallelism);

        long waitMillis = checkpointingIntervalMillis / 2;
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 0, waitMillis),
                new UnstableData("b", 1, waitMillis),
                new UnstableData("c", 1, waitMillis),
                new UnstableData("d", 1, waitMillis));

        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        KeyedStream<UnstableData, String> keyedData = processedData.keyBy((d) -> "");
        var concatenatedStream = keyedData.flatMap(new StatefulConcatenate());

        executeEnvAndCollectResult(concatenatedStream);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testManyFrequentStatefulFails(int parallelism) throws Exception {
        setParallelism(parallelism);

        long waitMillis = checkpointingIntervalMillis / 4;
        DataStream<UnstableData> data = env.fromElements(
                new UnstableData("a", 0, waitMillis),
                new UnstableData("b", 0, waitMillis),
                new UnstableData("c", 0, waitMillis),
                new UnstableData("d", 1, waitMillis),
                new UnstableData("e", 1, waitMillis),
                new UnstableData("f", 1, waitMillis),
                new UnstableData("g", 1, waitMillis),
                new UnstableData("h", 1, waitMillis)
        );

        DataStream<UnstableData> processedData = data.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        KeyedStream<UnstableData, String> keyedData = processedData.keyBy((unstableData) -> "");
        var concatenatedStream = keyedData.flatMap(new StatefulConcatenate());

        executeEnvAndCollectResult(concatenatedStream);
    }

    // must be at top level to support serialization
    public static final long defaultWaitMillis = checkpointingIntervalMillis / 2;

    public static final String groupA = "group A";
    public static final List<UnstableData> groupAElements = List.of(
            new UnstableData('a', 1, defaultWaitMillis, groupA),
            new UnstableData('b', 1, defaultWaitMillis, groupA),
            new UnstableData('c', 1, defaultWaitMillis, groupA),
            new UnstableData('d', 1, defaultWaitMillis, groupA));

    public static final String groupB = "group B";
    public static final List<UnstableData> groupBElements = List.of(
            new UnstableData('e', 1, defaultWaitMillis, groupB),
            new UnstableData('f', 1, defaultWaitMillis, groupB),
            new UnstableData('g', 1, defaultWaitMillis, groupB),
            new UnstableData('h', 1, defaultWaitMillis, groupB));

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testKeyedGroupsStatefulFails(int parallelism) throws Exception {
        setParallelism(parallelism);

        List<UnstableData> allElements = Stream.of(groupAElements, groupBElements)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        class StatefulConcatenateWithGroupsCheck extends StatefulConcatenate {
            @Override
            public void flatMap(UnstableData value, Collector<String> out) throws Exception {
                super.flatMap(value, out);

                String currentResult = concatResult.value();
                if (printLogs) {
                    if (checkFullGroupIsConcatenated(currentResult, groupAElements)) {
                        System.out.println("RESULT A ELEMENT: " + ANSI_BLUE + currentResult + ANSI_RESET);
                    }
                    if (checkFullGroupIsConcatenated(currentResult, groupBElements)) {
                        System.out.println("RESULT B ELEMENT: " + ANSI_RED + currentResult + ANSI_RESET);
                    }
                }
            }

            private boolean checkFullGroupIsConcatenated(String result, List<UnstableData> groupElements) {
                Set<String> resultNames = result.chars()
                        .mapToObj(c -> "" + ((char) c))
                        .collect(Collectors.toSet());
                Set<String> groupNames = groupElements.stream()
                        .map(element -> element.name)
                        .collect(Collectors.toSet());

                return resultNames.containsAll(groupNames);
            }
        }

        // declare initial stream
        DataStream<UnstableData> stream = env.fromCollection(allElements);

        // filter stream: possibly fail and recover
        DataStream<UnstableData> processedStream = stream.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        // concatenate all elements
        KeyedStream<UnstableData, String> keyedStream = processedStream.keyBy((unstableData) -> unstableData.group);
        var concatenateResults = keyedStream.flatMap(new StatefulConcatenateWithGroupsCheck());

        executeEnvAndCollectResult(concatenateResults);
    }

    public static class StatefulConcatenate extends RichFlatMapFunction<UnstableData, String> {

        protected transient ValueState<String> concatResult;

        @Override
        public void flatMap(UnstableData value, Collector<String> out) throws Exception {
            String currentResult = concatResult.value();
            if (currentResult == null) {
                currentResult = "";
            }
            currentResult += value.getName();
            concatResult.update(currentResult);

            out.collect(currentResult);

            if (printLogs) {
                System.out.println("CURRENT CONCAT RESULT: " + currentResult);
            }
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
        public final String group;

        // must be static otherwise each recover it will be recovered to initial value
        public static final Map<String, Integer> alreadyFailed = new ConcurrentHashMap<>();

        public static void resetAlreadyFailedState() {
            alreadyFailed.clear();
        }

        public UnstableData(String name, int failureTimes, long waitMillis, String group) {
            this.name = name;
            this.failureTimes = failureTimes;
            this.waitMillis = waitMillis;
            this.group = group;
            alreadyFailed.put(this.name, 0);
        }

        public UnstableData(String name, int failureTimes, long waitMillis) {
            this(name, failureTimes, waitMillis, "None");
        }

        public UnstableData(char name, int failureTimes, long waitMillis, String group) {
            this("" + name, failureTimes, waitMillis, group);
        }

        // accumulate logs
        public static final AtomicReference<String> validatedNamesLog = new AtomicReference<>("");

        public static final boolean printLogs = TestFlinkStatefulStreamsRecover.printLogs;

        private void log(String message) {
            if (printLogs) System.out.println(message);
        }

        public String getName() {
            return name;
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
