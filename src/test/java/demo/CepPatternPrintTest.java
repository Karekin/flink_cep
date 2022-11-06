package demo;

import demo.condition.StartCondition;
import demo.event.Event;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CepPatternPrintTest {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // DataStream Source
        DataStreamSource<Event> sourceDS = env.addSource(new CepDemo2.MyEventSource2());
        KeyedStream<Event, String> keyedStream = sourceDS.keyBy(Event::getName);

        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start")
                        .where(new StartCondition("productionId == 11 && action == 0"))
                        .times(3,3)
                        .followedBy("middle")
                        .where(new StartCondition("productionId == 11 && action == 1"))
                        .notFollowedBy("end")
                        .where(new StartCondition("productionId == 11 && action == 2"))
                        .within(Time.seconds(10));
        printTestPattern(pattern);

    }

    public static void printTestPattern(Pattern<?, ?> pattern) throws JsonProcessingException {
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }
}
