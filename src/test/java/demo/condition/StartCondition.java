package demo.condition;

import demo.event.Event;
import org.apache.flink.cep.dynamic.condition.AviatorCondition;

public class StartCondition extends AviatorCondition<Event> {

    public StartCondition(String expression) {
        super(expression);
    }
}
