package ru.hse.flinkanomaly;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFlinkStreams {

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setUpEnvironment() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    void testPrintMapUppercase() throws Exception {
        DataStream<String> stream = env.fromElements("Conquer", "Flink", "!");
        SingleOutputStreamOperator<String> upperCase = stream.map(String::toUpperCase);
        upperCase.print();
        env.execute();
    }

}
