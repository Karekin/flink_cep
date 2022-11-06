package demo;

import demo.dynamic.JDBCPeriodicPatternProcessorDiscovererFactory;
import demo.event.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CepDemo2 {

    /**
     * select * from dynamic_cep where id='2' and version = 1;
     * 同一个客户，连续浏览商品11，大于等于3次的，并最后购买，并紧急着分享，在10秒内，
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // DataStream Source
        DataStreamSource<Event> sourceDS = env.addSource(new MyEventSource2());
        KeyedStream<Event, String> keyedStream = sourceDS.keyBy(Event::getName);

        // Dynamic CEP patterns
        SingleOutputStreamOperator<String> output =
                CEP.dynamicPatterns(
                        keyedStream,
                        new JDBCPeriodicPatternProcessorDiscovererFactory<>(
                                Constants.JDBC_URL,
                                Constants.JDBC_DRIVE,
                                Constants.TABLE_NAME,
                                null,
                                5000L),
                        TimeBehaviour.ProcessingTime,
                        TypeInformation.of(new TypeHint<String>() {}));
        // Print output stream in taskmanager's stdout
        output.print("符合cep-> ");
        // Compile and submit the job
        env.execute("CEPDemo2");
    }


    public static class MyEventSource2 implements SourceFunction<Event> {

        //要一直执行,不断生成数据
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
//            // 用户名
//            List<String> nameList = Lists.newArrayList("aaa", "bbb", "fff");
//            // 用户动作，取值如下：0代表浏览操作。1代表购买动作。2代表分享操作。
//            List<Integer> actionList = Lists.newArrayList(0, 1, 2);
//            // 产品id
//            List<Integer> productIdList = Lists.newArrayList(11, 12, 13);
//            // 随机数
//            Random random = new Random();
//            int id =1;
//            while (true){
//                Event event = new Event(
//                        id,
//                        nameList.get(random.nextInt(nameList.size())),
//                        actionList.get(random.nextInt(actionList.size())),
//                        productIdList.get(random.nextInt(productIdList.size())),
//                        System.currentTimeMillis()
//                        );
//                ctx.collect(event);
//                id ++;
//                Thread.sleep(200);
//                System.out.println("原始数据 -> " + event);
//            }

            ctx.collect(new Event(1,"aaa", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(2,"aaa", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(3,"aaa", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(4,"aaa", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(5,"aaa", 0, 12, System.currentTimeMillis()));
            ctx.collect(new Event(6,"aaa", 1, 11, System.currentTimeMillis()));
            ctx.collect(new Event(7,"aaa", 0, 13, System.currentTimeMillis()));
            ctx.collect(new Event(8,"aaa", 2, 11, System.currentTimeMillis()));

            ctx.collect(new Event(6,"bbb", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(7,"bbb", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(8,"bbb", 0, 12, System.currentTimeMillis()));
            ctx.collect(new Event(9,"bbb", 0, 12, System.currentTimeMillis()));
            ctx.collect(new Event(10,"bbb", 0, 13, System.currentTimeMillis()));

            ctx.collect(new Event(11,"ccc", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(12,"ccc", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(13,"ccc", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(14,"ccc", 1, 11, System.currentTimeMillis()));
            ctx.collect(new Event(15,"ccc", 2, 11, System.currentTimeMillis()));

            ctx.collect(new Event(16,"ddd", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(17,"ddd", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(18,"ddd", 0, 13, System.currentTimeMillis()));
            ctx.collect(new Event(19,"ddd", 1, 13, System.currentTimeMillis()));
            ctx.collect(new Event(20,"ddd", 2, 13, System.currentTimeMillis()));

            ctx.collect(new Event(21,"eee", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(22,"eee", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(23,"eee", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(24,"eee", 1, 11, System.currentTimeMillis()));
            ctx.collect(new Event(25,"eee", 0, 13, System.currentTimeMillis()));

            ctx.collect(new Event(21,"ffff", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(22,"ffff", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(23,"ffff", 0, 11, System.currentTimeMillis()));
            ctx.collect(new Event(24,"ffff", 0, 12, System.currentTimeMillis()));
            ctx.collect(new Event(25,"ffff", 0, 13, System.currentTimeMillis()));
        }

        @Override
        public void cancel() {

        }

    }

}
