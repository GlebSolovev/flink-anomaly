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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Main {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_RED = "\u001B[31m";

    public static final long defaultWaitMillis = 500;

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

    public static final List<UnstableData> allElements = Stream.of(groupAElements, groupBElements)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up checkpoints and recovering
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.seconds(1)));

        // set up parallelism
        int parallelism = 2;
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);

        // declare initial stream
        DataStream<UnstableData> stream = env.fromCollection(allElements);

        // filter stream: possibly fail and recover
        DataStream<UnstableData> processedStream = stream.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        // concatenate all elements
        KeyedStream<UnstableData, String> keyedStream = processedStream.keyBy((unstableData) -> unstableData.group);
        keyedStream.flatMap(new StatefulConcatenate());

        // execute test
        env.execute();
    }

    public static class StatefulConcatenate extends RichFlatMapFunction<UnstableData, String> {

        private transient ValueState<String> concatResult;

        @Override
        public void flatMap(UnstableData value, Collector<String> out) throws Exception {
            String currentResult = concatResult.value();
            if (currentResult == null) {
                currentResult = "";
            }
            currentResult += value.name;
            concatResult.update(currentResult);

            System.out.println("CURRENT CONCAT RESULT: " + currentResult);

            if (checkFullGroupIsConcatenated(currentResult, groupAElements)) {
                System.out.println("RESULT A ELEMENT: " + ANSI_BLUE + currentResult + ANSI_RESET);
            }
            if (checkFullGroupIsConcatenated(currentResult, groupBElements)) {
                System.out.println("RESULT B ELEMENT: " + ANSI_RED + currentResult + ANSI_RESET);
            }
        }

        private static boolean checkFullGroupIsConcatenated(String result, List<UnstableData> groupElements) {
            Set<String> resultNames = result.chars()
                    .mapToObj(c -> "" + ((char) c))
                    .collect(Collectors.toSet());
            Set<String> groupNames = groupElements.stream()
                    .map(element -> element.name)
                    .collect(Collectors.toSet());

            return resultNames.containsAll(groupNames);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<String> descriptor =
                    new ValueStateDescriptor<>("concatenate", TypeInformation.of(new TypeHint<>() {
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

        public UnstableData(char name, int failureTimes, long waitMillis, String group) {
            this.name = "" + name;
            this.failureTimes = failureTimes;
            this.waitMillis = waitMillis;
            this.group = group;
            alreadyFailed.put(this.name, 0);
        }

        // method fails this.failureTimes times for each this, then passes
        // takes this.waitMillis each call
        public boolean waitValidateOrFail() throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(waitMillis);

            int thisAlreadyFailed = alreadyFailed.get(name);
            if (thisAlreadyFailed < failureTimes) {
                alreadyFailed.put(name, thisAlreadyFailed + 1);
                throw new UnstableDataFailedException();
            }
            return true;
        }
    }

    public static class UnstableDataFailedException extends RuntimeException {
    }
}
