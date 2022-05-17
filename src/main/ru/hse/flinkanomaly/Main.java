package ru.hse.flinkanomaly;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.fromElements("Conquer", "Flink", "!");

        SingleOutputStreamOperator<String> upperCase = dataStream.map(String::toUpperCase);
        upperCase.print();
        env.execute();
    }
}