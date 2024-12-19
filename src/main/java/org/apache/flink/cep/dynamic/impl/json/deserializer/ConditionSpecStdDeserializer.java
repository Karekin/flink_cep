/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.dynamic.impl.json.deserializer;

import org.apache.flink.cep.dynamic.impl.json.spec.AviatorConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.ClassConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.RichAndConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.RichNotConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.RichOrConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.SubTypeConditionSpec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ConditionSpecStdDeserializer 是一个自定义的 JSON 反序列化器，用于将 JSON 数据反序列化为 {@link ConditionSpec} 对象。
 *
 * 该类继承自 Jackson 的 {@link StdDeserializer}，通过解析 JSON 数据的结构和字段，动态生成不同类型的 ConditionSpec 对象。
 *
 * 支持以下 ConditionSpec 子类的反序列化：
 * - {@link RichAndConditionSpec}
 * - {@link RichOrConditionSpec}
 * - {@link RichNotConditionSpec}
 * - {@link SubTypeConditionSpec}
 * - {@link ClassConditionSpec}
 * - {@link AviatorConditionSpec}
 */

public class ConditionSpecStdDeserializer extends StdDeserializer<ConditionSpec> {

    /**
     * ConditionSpecStdDeserializer 的单例实例。
     *
     * 避免频繁创建多个实例，减少资源开销。
     */
    public static final ConditionSpecStdDeserializer INSTANCE = new ConditionSpecStdDeserializer();

    /**
     * 序列化的版本 ID，用于确保对象序列化与反序列化的一致性。
     */
    private static final long serialVersionUID = 1L;


    /**
     * 无参构造函数，用于创建 ConditionSpecStdDeserializer 对象。
     *
     * 默认调用父类的构造函数并设置类型为 null。
     */
    public ConditionSpecStdDeserializer() {
        this(null);
    }


    /**
     * 带参构造函数，用于创建 ConditionSpecStdDeserializer 对象。
     *
     * @param vc 要反序列化的类类型。
     */
    public ConditionSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }


    /**
     * 反序列化方法，用于将 JSON 数据转换为 {@link ConditionSpec} 对象。
     *
     * 根据 JSON 数据中的 "type" 字段判断具体的 ConditionSpec 类型，并动态生成对应的子类对象。
     *
     * @param jsonParser JSON 解析器，用于读取 JSON 数据。
     * @param deserializationContext 反序列化上下文，提供额外的反序列化配置和工具。
     * @return 反序列化后的 {@link ConditionSpec} 对象。
     * @throws IOException 如果 JSON 解析出错或字段缺失。
     */
    @Override
    public ConditionSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 数据并转换为树状结构
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 获取条件类型字段（type），并转换为 ConditionType 枚举
        ConditionSpec.ConditionType type =
                ConditionSpec.ConditionType.valueOf(node.get("type").asText());

        // 根据条件类型选择反序列化逻辑
        if (type.equals(ConditionSpec.ConditionType.CLASS)) {
            // 获取类名字段
            String className = node.get("className").asText();

            // 如果包含嵌套条件
            if (node.get("nestedConditions") != null) {
                List<ConditionSpec> nestedConditions = new ArrayList<>();
                // 遍历嵌套条件节点
                Iterator<JsonNode> embeddedElementNames = node.get("nestedConditions").elements();
                while (embeddedElementNames.hasNext()) {
                    JsonNode jsonNode = embeddedElementNames.next();
                    // 将嵌套节点反序列化为 ConditionSpec
                    ConditionSpec embedNode =
                            jsonParser.getCodec().treeToValue(jsonNode, ConditionSpec.class);
                    nestedConditions.add(embedNode);
                }
                // 根据类名创建特定的条件规格对象
                if (className.endsWith("flink.cep.pattern.conditions.RichAndCondition")) {
                    return new RichAndConditionSpec(nestedConditions);
                } else if (className.endsWith("flink.cep.pattern.conditions.RichOrCondition")) {
                    return new RichOrConditionSpec(nestedConditions);
                } else if (className.endsWith("flink.cep.pattern.conditions.RichNotCondition")) {
                    return new RichNotConditionSpec(nestedConditions);
                }
            }
            // 如果包含子类名字段
            else if (node.get("subClassName") != null) {
                return new SubTypeConditionSpec(className, node.get("subClassName").asText());
            }
            // 默认返回 ClassConditionSpec
            return new ClassConditionSpec(className);
        }
        // 如果是 Aviator 类型的条件
        else if (type.equals(ConditionSpec.ConditionType.AVIATOR)) {
            if (node.get("expression") != null) {
                // 返回 AviatorConditionSpec
                return new AviatorConditionSpec(node.get("expression").asText());
            } else {
                // 如果表达式字段为空，抛出异常
                throw new IllegalArgumentException(
                        "The expression field of Aviator Condition cannot be null!");
            }
        }
        // 如果条件类型不支持，抛出异常
        throw new IllegalStateException("Unsupported Condition type: " + type);
    }

}
