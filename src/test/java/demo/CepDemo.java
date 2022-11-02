package demo;

import demo.dynamic.JDBCPeriodicPatternProcessorDiscovererFactory;
import demo.event.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.Random;

public class CepDemo {

    public static void printTestPattern(Pattern<?, ?> pattern) throws JsonProcessingException {
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }

    public static void checkArg(String argName, MultipleParameterTool params) {
        if (!params.has(argName)) {
            throw new IllegalArgumentException(argName + " must be set!");
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // DataStream Source
        DataStreamSource<Event> source = env.addSource(new MyEventSource());
        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                source.keyBy(
                        new KeySelector<Event, Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
                                return Tuple2.of(value.getId(), value.getProductionId());
                            }
                        });

//        Pattern<Event, Event> pattern =
//                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
//                        .where(new StartCondition("action == 0"))
//                        .timesOrMore(3)
//                        .followedBy("end")
//                        .where(new EndCondition());
//        printTestPattern(pattern);

        // Dynamic CEP patterns
        SingleOutputStreamOperator<String> output =
                CEP.dynamicPatterns(
                        keyedStream,
                        new JDBCPeriodicPatternProcessorDiscovererFactory<>(
                                Constants.JDBC_URL,
                                Constants.JDBC_DRIVE,
                                Constants.TABLE_NAME,
                                null,
                                1000L),
                        TimeBehaviour.ProcessingTime,
                        TypeInformation.of(new TypeHint<String>() {}));
        // Print output stream in taskmanager's stdout
        output.print();
        // Compile and submit the job
        env.execute("CEPDemo");
    }


    public static class MyEventSource extends RichParallelSourceFunction<Event> {
        private Boolean flag = true;
        //编写开启资源的代码
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }
        //要一直执行,不断生成数据
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            List<String> nameList = Lists.newArrayList("张三","李四","王五","赵六");
            while (flag){
                int id = random.nextInt(1000);
                String name = nameList.get(random.nextInt(nameList.size()-1));
                int productionId = random.nextInt(10);
                int action = random.nextInt(5);
                long eventTime = System.currentTimeMillis();
                Event event = new Event(id, name, productionId, action, eventTime);
                ctx.collect(event);
                Thread.sleep(500);
            }
        }
        @Override
        public void cancel() {
            flag = false;
        }
        @Override
        public void close() throws Exception {
            super.close();
        }
    }
}
