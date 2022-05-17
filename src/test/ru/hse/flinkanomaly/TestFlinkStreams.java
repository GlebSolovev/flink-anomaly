package ru.hse.flinkanomaly;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestFlinkStreams {

    @TempDir
    File tempDir;

    private StreamExecutionEnvironment env;

    private void applyEnvironmentSettings() {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(1), CheckpointingMode.AT_LEAST_ONCE);
        env.setParallelism(1);
        env.setMaxParallelism(1);
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

    @Test
    void printFilteredFlintstones() throws Exception {
        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));
        DataStream<Person> adults = flintstones.filter((FilterFunction<Person>) person -> person.age >= 18);

        adults.print();
        env.execute();
    }


    @Test
    void readFromAndSinkToFile() throws Exception {
        File inputFile = new File(tempDir, "input.txt");
        List<String> inputLines = Arrays.asList("love", "flink", "hardly");
        Files.write(inputFile.toPath(), inputLines);

        DataStream<String> lines = env.readTextFile(inputFile.getAbsolutePath());

        String outputDirPath = tempDir.toPath().resolve("readFromFileOutput").toString();
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputDirPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        lines.addSink(sink);
        env.execute();

        try (var paths = Files.walk(Paths.get(outputDirPath))) {
            var filePaths = paths.filter(Files::isRegularFile).collect(Collectors.toList());
            Assertions.assertEquals(1, filePaths.size());
            var outputFilePath = filePaths.get(0);

            List<String> outputLines = Files.readAllLines(outputFilePath);
            Assertions.assertEquals(inputLines, outputLines);
        }
    }

    public static class Person {
        public final String name;
        public final Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name + ": age " + this.age.toString();
        }
    }
}
