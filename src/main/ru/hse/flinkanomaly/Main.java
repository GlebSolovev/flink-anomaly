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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up checkpoints and recovering
        long checkpointingIntervalMillis = 1000;
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(10, 0));

        // set up parallelism
        int parallelism = 2;
        env.setParallelism(parallelism);
        env.setMaxParallelism(parallelism);

        // declare initial stream
        long waitMillis = checkpointingIntervalMillis / 4;
        DataStream<UnstableData> stream = env.fromElements(
                new UnstableData("a", 0, waitMillis),
                new UnstableData("b", 0, waitMillis),
                new UnstableData("c", 0, waitMillis),
                new UnstableData("d", 1, waitMillis),
                new UnstableData("e", 1, waitMillis)
        );

        // filter stream: possibly fail and recover
        DataStream<UnstableData> processedStream = stream.filter(
                (FilterFunction<UnstableData>) UnstableData::waitValidateOrFail);

        // concatenate all elements
        KeyedStream<UnstableData, String> keyedStream = processedStream.keyBy((unstableData) -> "");
        var res = keyedStream.flatMap(new StatefulConcatenate());
        res.print();

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

            out.collect(currentResult);
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

        // must be static otherwise each recover it will be recovered to initial value
        public static final Map<String, Integer> alreadyFailed = new ConcurrentHashMap<>();

        public UnstableData(String name, int failureTimes, long waitMillis) {
            this.name = name;
            this.failureTimes = failureTimes;
            this.waitMillis = waitMillis;
        }

        // method fails this.failureTimes times for each this, then passes
        // takes this.waitMillis each call
        public boolean waitValidateOrFail() throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(waitMillis);

            alreadyFailed.putIfAbsent(name, 0);
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
