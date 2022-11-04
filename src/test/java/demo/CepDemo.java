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
import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

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
        output.print("符合cep-> ");
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
            List<String> nameList = Lists.newArrayList("张三","李四","王五","middle");
            List<Integer> actionList = Lists.newArrayList(0,0,0,0,0);
            long eventTime = 0;
            while (flag){
//                int id = random.nextInt(1000);
//                String name = nameList.get(random.nextInt(nameList.size()));
//                int productionId = random.nextInt(10);
//                int action = actionList.get(random.nextInt(actionList.size()));
//                long eventTime = System.currentTimeMillis();
//                Event event = new Event(id, name, productionId, action, eventTime);

                eventTime +=1;
                Event event = new Event(1, "Ken", 0, 1, eventTime);

                ctx.collect(event);
                System.out.println("原数据-> " + event);
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

    @Test
    public void test() throws Exception {
        ObjectMapper objectMapper =
                new ObjectMapper()
                        .registerModule(
                                new SimpleModule()
                                        .addDeserializer(
                                                ConditionSpec.class,
                                                ConditionSpecStdDeserializer.INSTANCE)
                                        .addDeserializer(Time.class, TimeStdDeserializer.INSTANCE)
                                        .addDeserializer(
                                                NodeSpec.class, NodeSpecStdDeserializer.INSTANCE));

        String patternStr = "{\n" +
                "  \"name\": \"end\",\n" +
                "  \"quantifier\": {\n" +
                "    \"consumingStrategy\": \"SKIP_TILL_NEXT\",\n" +
                "    \"properties\": [\n" +
                "      \"SINGLE\"\n" +
                "    ],\n" +
                "    \"times\": null,\n" +
                "    \"untilCondition\": null\n" +
                "  },\n" +
                "  \"condition\": null,\n" +
                "  \"nodes\": [\n" +
                "    {\n" +
                "      \"name\": \"end\",\n" +
                "      \"quantifier\": {\n" +
                "        \"consumingStrategy\": \"SKIP_TILL_NEXT\",\n" +
                "        \"properties\": [\n" +
                "          \"SINGLE\"\n" +
                "        ],\n" +
                "        \"times\": null,\n" +
                "        \"untilCondition\": null\n" +
                "      },\n" +
                "      \"condition\": {\n" +
                "        \"className\": \"demo.condition.EndCondition\",\n" +
                "        \"type\": \"CLASS\"\n" +
                "      },\n" +
                "      \"type\": \"ATOMIC\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"middle\",\n" +
                "      \"quantifier\": {\n" +
                "        \"consumingStrategy\": \"SKIP_TILL_NEXT\",\n" +
                "        \"properties\": [\n" +
                "          \"LOOPING\"\n" +
                "        ],\n" +
                "        \"times\": {\n" +
                "          \"from\": 3,\n" +
                "          \"to\": 3,\n" +
                "          \"windowTime\": null\n" +
                "        },\n" +
                "        \"untilCondition\": null\n" +
                "      },\n" +
                "      \"condition\": {\n" +
                "        \"className\": \"demo.condition.MiddleCondition\",\n" +
                "        \"type\": \"CLASS\"\n" +
                "      },\n" +
                "      \"type\": \"ATOMIC\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"start\",\n" +
                "      \"quantifier\": {\n" +
                "        \"consumingStrategy\": \"SKIP_TILL_NEXT\",\n" +
                "        \"properties\": [\n" +
                "          \"SINGLE\",\n" +
                "          \"OPTIONAL\"\n" +
                "        ],\n" +
                "        \"times\": null,\n" +
                "        \"untilCondition\": null\n" +
                "      },\n" +
                "      \"condition\": {\n" +
                "        \"className\": \"demo.condition.StartCondition\",\n" +
                "        \"type\": \"CLASS\"\n" +
                "      },\n" +
                "      \"type\": \"ATOMIC\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"edges\": [\n" +
                "    {\n" +
                "      \"source\": \"middle\",\n" +
                "      \"target\": \"end\",\n" +
                "      \"type\": \"NOT_FOLLOW\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"source\": \"start\",\n" +
                "      \"target\": \"middle\",\n" +
                "      \"type\": \"SKIP_TILL_NEXT\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"window\": {\n" +
                "    \"type\": \"FIRST_AND_LAST\",\n" +
                "    \"time\": {\n" +
                "      \"unit\": \"MINUTES\",\n" +
                "      \"size\": 10\n" +
                "    }\n" +
                "  },\n" +
                "  \"afterMatchStrategy\": {\n" +
                "    \"type\": \"NO_SKIP\",\n" +
                "    \"patternName\": null\n" +
                "  },\n" +
                "  \"type\": \"COMPOSITE\",\n" +
                "  \"version\": 1\n" +
                "}";
        GraphSpec graphSpec =
                objectMapper.readValue(patternStr, GraphSpec.class);
    }
}
