package demo.serializer;

import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

public class GraphSpecTest {

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
