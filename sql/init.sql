#1、新建规则数据源和表，规则的唯一性是由 id+version 组成；
create database xp_flink_dev;
CREATE TABLE `dynamic_cep` (
    `id` varchar(40) DEFAULT NULL,
    `version` int(8) DEFAULT NULL,
    `pattern` text,
    `function` text,
    `is_deleted` int(1) DEFAULT '0',
    KEY `index_id_version` (`id`,`version`)
);


# 2、新增id=1，version=1 的规则
# INSERT INTO dynamic_cep(`id`,`version`,`pattern`,`function`) values('1',1,'{"name":"end","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["SINGLE"],"times":null,"untilCondition":null},"condition":null,"nodes":[{"name":"end","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["SINGLE"],"times":null,"untilCondition":null},"condition":{"className":"demo.condition.EndCondition","type":"CLASS"},"type":"ATOMIC"},{"name":"start","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["LOOPING"],"times":{"from":3,"to":3,"windowTime":null},"untilCondition":null},"condition":{"expression":"action == 0","type":"AVIATOR"},"type":"ATOMIC"}],"edges":[{"source":"start","target":"end","type":"SKIP_TILL_NEXT"}],"window":null,"afterMatchStrategy":{"type":"SKIP_PAST_LAST_EVENT","patternName":null},"type":"COMPOSITE","version":1}','demo.dynamic.DemoPatternProcessFunction');

# 3、启动应用 CepDemo.main 方法，查看应用日记输出，符合cep的数据

# 4、新增id=1，version=2 的规则，查看应用日记输出，符合cep的数据
# INSERT INTO dynamic_cep(`id`,`version`,`pattern`,`function`) values('1',2,'{"name":"end","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["SINGLE"],"times":null,"untilCondition":null},"condition":null,"nodes":[{"name":"end","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["SINGLE"],"times":null,"untilCondition":null},"condition":{"className":"demo.condition.EndCondition","type":"CLASS"},"type":"ATOMIC"},{"name":"start","quantifier":{"consumingStrategy":"SKIP_TILL_NEXT","properties":["LOOPING"],"times":{"from":5,"to":5,"windowTime":null},"untilCondition":null},"condition":{"expression":"action == 0 || action == 2","type":"AVIATOR"},"type":"ATOMIC"}],"edges":[{"source":"start","target":"end","type":"SKIP_TILL_NEXT"}],"window":null,"afterMatchStrategy":{"type":"SKIP_PAST_LAST_EVENT","patternName":null},"type":"COMPOSITE","version":2}','demo.dynamic.DemoPatternProcessFunction');

# 5、删除id=1，version=1 的规则，查看应用日记输出，符合cep的数据
# delete from dynamic_cep where id='1' and version =1;


# 同一个客户，连续浏览商品11，大于等于3次的，并最后购买，并紧急着分享，在10秒内，
INSERT INTO `dynamic_cep`(`id`, `version`, `pattern`, `function`, `is_deleted`) VALUES ('2', 1, '{\r\n    \"afterMatchStrategy\": {\r\n        \"type\": \"NO_SKIP\"\r\n    },\r\n    \"edges\": [\r\n        {\r\n            \"source\": \"middle\",\r\n            \"target\": \"end\",\r\n            \"type\": \"STRICT\"\r\n        },\r\n        {\r\n            \"source\": \"start\",\r\n            \"target\": \"middle\",\r\n            \"type\": \"SKIP_TILL_NEXT\"\r\n        }\r\n    ],\r\n    \"name\": \"end\",\r\n    \"nodes\": [\r\n        {\r\n            \"condition\": {\r\n                \"expression\": \"productionId == 11 && action == 2\",\r\n                \"type\": \"AVIATOR\"\r\n            },\r\n            \"name\": \"end\",\r\n            \"quantifier\": {\r\n                \"consumingStrategy\": \"SKIP_TILL_NEXT\",\r\n                \"properties\": [\r\n                    \"SINGLE\"\r\n                ]\r\n            },\r\n            \"type\": \"ATOMIC\"\r\n        },\r\n        {\r\n            \"condition\": {\r\n                \"expression\": \"productionId == 11 && action == 1\",\r\n                \"type\": \"AVIATOR\"\r\n            },\r\n            \"name\": \"middle\",\r\n            \"quantifier\": {\r\n                \"consumingStrategy\": \"SKIP_TILL_NEXT\",\r\n                \"properties\": [\r\n                    \"SINGLE\"\r\n                ]\r\n            },\r\n            \"type\": \"ATOMIC\"\r\n        },\r\n        {\r\n            \"condition\": {\r\n                \"expression\": \"productionId == 11 && action == 0\",\r\n                \"type\": \"AVIATOR\"\r\n            },\r\n            \"name\": \"start\",\r\n            \"quantifier\": {\r\n                \"consumingStrategy\": \"SKIP_TILL_NEXT\",\r\n                \"properties\": [\r\n                    \"TIMES\"\r\n                ],\r\n                \"times\": {\r\n                    \"from\": 3,\r\n                    \"to\": 3\r\n                }\r\n            },\r\n            \"type\": \"ATOMIC\"\r\n        }\r\n    ],\r\n    \"quantifier\": {\r\n        \"consumingStrategy\": \"SKIP_TILL_NEXT\",\r\n        \"properties\": [\r\n            \"SINGLE\"\r\n        ]\r\n    },\r\n    \"type\": \"COMPOSITE\",\r\n    \"version\": 1,\r\n    \"window\": {\r\n        \"time\": {\r\n            \"size\": 10,\r\n            \"unit\": \"SECONDS\"\r\n        },\r\n        \"type\": \"FIRST_AND_LAST\"\r\n    }\r\n}', 'demo.dynamic.DemoPatternProcessFunction', 0);


