create database xp_flink_dev;

create table dynamic_cep(
                            id varchar(40),
                            version int(8),
                            pattern text,
                            `function` text
);


INSERT INTO xp_flink_dev.dynamic_cep
(id, version, pattern, `function`)
VALUES('1', 1, '{"name": "end","quantifier": {"consumingStrategy": "SKIP_TILL_NEXT","properties": ["SINGLE"],"times": null,"untilCondition": null},"condition": null,"nodes": [{"name": "end","quantifier": {"consumingStrategy": "SKIP_TILL_NEXT","properties": ["SINGLE"],"times": null,"untilCondition": null},"condition": {"className": "com.alibaba.ververica.cep.demo.condition.EndCondition","type": "CLASS"},"type": "ATOMIC"},{"name": "start","quantifier": {"consumingStrategy": "SKIP_TILL_NEXT","properties": ["SINGLE"],"times": null,"untilCondition": null},"condition": {"args": "eventArgs.detail.price > 10000","className": "com.alibaba.ververica.cep.demo.condition.StartCondition","type": "CUSTOM_ARGS"},"type": "ATOMIC"}],"edges": [{"source": "start","target": "end","type": "SKIP_TILL_NEXT"}],"window": null,"afterMatchStrategy": {"type": "SKIP_PAST_LAST_EVENT","patternName": null},"type": "COMPOSITE","version": 1}', 'com.alibaba.ververica.cep.demo.dynamic.DemoPatternProcessFunction');

