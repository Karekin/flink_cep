package demo.serializer;


import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 测试类：测试 ConditionSpecStdDeserializer 的 JSON 反序列化功能。
 */
public class ConditionSpecStdDeserializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ConditionSpec.class, ConditionSpecStdDeserializer.INSTANCE);
        objectMapper.registerModule(module);
    }

    // 测试 AviatorConditionSpec：验证表达式字段的反序列化。
    @Test
    public void testAviatorConditionSpec() throws Exception {
        String json = "{" +
                "\"type\": \"AVIATOR\", " +
                "\"expression\": \"a > 10\"}";

        ConditionSpec conditionSpec = objectMapper.readValue(json, ConditionSpec.class);

        assertTrue(conditionSpec instanceof AviatorConditionSpec);
        AviatorConditionSpec aviatorConditionSpec = (AviatorConditionSpec) conditionSpec;
        assertEquals("a > 10", aviatorConditionSpec.getExpression());
    }

    // 测试 ClassConditionSpec：验证仅包含类名的条件。
    @Test
    public void testClassConditionSpec() throws Exception {
        String json = "{" +
                "\"type\": \"CLASS\", " +
                "\"className\": \"com.example.MyCondition\"}";

        ConditionSpec conditionSpec = objectMapper.readValue(json, ConditionSpec.class);

        assertTrue(conditionSpec instanceof ClassConditionSpec);
        ClassConditionSpec classConditionSpec = (ClassConditionSpec) conditionSpec;
        assertEquals("com.example.MyCondition", classConditionSpec.getClassName());
    }

    // 测试 SubTypeConditionSpec：验证包含子类名的条件。
    @Test
    public void testSubTypeConditionSpec() throws Exception {
        String json = "{" +
                "\"type\": \"CLASS\", " +
                "\"className\": \"com.example.MySuperCondition\", " +
                "\"subClassName\": \"com.example.MySubCondition\"}";

        ConditionSpec conditionSpec = objectMapper.readValue(json, ConditionSpec.class);

        assertTrue(conditionSpec instanceof SubTypeConditionSpec);
        SubTypeConditionSpec subTypeConditionSpec = (SubTypeConditionSpec) conditionSpec;
        assertEquals("com.example.MySuperCondition", subTypeConditionSpec.getClassName());
        assertEquals("com.example.MySubCondition", subTypeConditionSpec.getSubClassName());
    }

    // 测试 RichAndConditionSpec：验证嵌套多个子条件的“与”逻辑。
    @Test
    public void testRichAndConditionSpec() throws Exception {
        String json = "{" +
                "\"type\": \"CLASS\", " +
                "\"className\": \"flink.cep.pattern.conditions.RichAndCondition\", " +
                "\"nestedConditions\": [" +
                "{" +
                "\"type\": \"AVIATOR\", " +
                "\"expression\": \"a > 10\"}, " +
                "{" +
                "\"type\": \"AVIATOR\", " +
                "\"expression\": \"b < 20\"}" +
                "]}";

        ConditionSpec conditionSpec = objectMapper.readValue(json, ConditionSpec.class);

        assertTrue(conditionSpec instanceof RichAndConditionSpec);
        RichAndConditionSpec andConditionSpec = (RichAndConditionSpec) conditionSpec;
        assertEquals(2, andConditionSpec.getNestedConditions().size());
        assertTrue(andConditionSpec.getNestedConditions().get(0) instanceof AviatorConditionSpec);
        assertTrue(andConditionSpec.getNestedConditions().get(1) instanceof AviatorConditionSpec);
    }

    // 测试 RichOrConditionSpec：验证嵌套多个子条件的“或”逻辑。
    @Test
    public void testRichOrConditionSpec() throws Exception {
        String json = "{" +
                "\"type\": \"CLASS\", " +
                "\"className\": \"flink.cep.pattern.conditions.RichOrCondition\", " +
                "\"nestedConditions\": [" +
                "{" +
                "\"type\": \"AVIATOR\", " +
                "\"expression\": \"a == 5\"}, " +
                "{" +
                "\"type\": \"AVIATOR\", " +
                "\"expression\": \"b > 15\"}" +
                "]}";

        ConditionSpec conditionSpec = objectMapper.readValue(json, ConditionSpec.class);

        assertTrue(conditionSpec instanceof RichOrConditionSpec);
        RichOrConditionSpec orConditionSpec = (RichOrConditionSpec) conditionSpec;
        assertEquals(2, orConditionSpec.getNestedConditions().size());
        assertTrue(orConditionSpec.getNestedConditions().get(0) instanceof AviatorConditionSpec);
        assertTrue(orConditionSpec.getNestedConditions().get(1) instanceof AviatorConditionSpec);
    }

    @Test
    public void testInvalidConditionType() {
        String json = "{" +
                "\"type\": \"INVALID\", " +
                "\"expression\": \"c == 1\"}";

        assertThrows(IllegalArgumentException.class, () -> {
            objectMapper.readValue(json, ConditionSpec.class);
        });
    }

}

