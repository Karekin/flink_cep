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

import org.apache.flink.cep.dynamic.impl.json.spec.AfterMatchSkipStrategySpec;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.EdgeSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.QuantifierSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.WindowSpec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * NodeSpecStdDeserializer 是一个自定义的 JSON 反序列化器，用于将 JSON 数据反序列化为 {@link NodeSpec} 对象。
 *
 * 该类继承自 Jackson 的 {@link StdDeserializer}，通过解析 JSON 数据，动态生成不同类型的 NodeSpec 对象。
 * 支持以下类型的反序列化：
 * - {@link NodeSpec}：普通节点。
 * - {@link GraphSpec}：复合节点（COMPOSITE 类型）。
 *
 * 复合节点包含嵌套的节点（nodes）、边（edges）以及窗口（window）等属性。
 */
public class NodeSpecStdDeserializer extends StdDeserializer<NodeSpec> {
    /**
     * 单例实例，用于全局复用此反序列化器，避免频繁创建多个实例。
     */
    public static final NodeSpecStdDeserializer INSTANCE = new NodeSpecStdDeserializer();

    /**
     * 序列化的版本 ID，用于确保对象序列化与反序列化的一致性。
     */
    private static final long serialVersionUID = 1L;


    /**
     * 无参构造函数，用于创建 NodeSpecStdDeserializer 对象。
     *
     * 默认调用父类构造函数并设置类型为 null。
     */
    public NodeSpecStdDeserializer() {
        this(null);
    }

    /**
     * 带参构造函数，用于创建 NodeSpecStdDeserializer 对象。
     *
     * @param vc 要反序列化的类类型。
     */
    public NodeSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }


    /**
     * 反序列化方法，用于将 JSON 数据转换为 {@link NodeSpec} 对象。
     *
     * 根据 JSON 数据中的 "type" 字段判断具体的 NodeSpec 类型，并动态生成普通节点或复合节点。
     *
     * @param jsonParser JSON 解析器，用于读取 JSON 数据。
     * @param deserializationContext 反序列化上下文，提供额外的反序列化配置和工具。
     * @return 反序列化后的 {@link NodeSpec} 对象。
     * @throws IOException 如果 JSON 解析出错或字段缺失。
     */
    @Override
    public NodeSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 数据并转换为树状结构
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 获取节点类型字段，并转换为枚举类型
        NodeSpec.PatternNodeType type = NodeSpec.PatternNodeType.valueOf(node.get("type").asText());

        // 获取节点名称
        String name = node.get("name").asText();

        // 反序列化量词规格字段
        QuantifierSpec quantifierSpec =
                jsonParser.getCodec().treeToValue(node.get("quantifier"), QuantifierSpec.class);

        // 反序列化条件规格字段
        ConditionSpec conditionSpec =
                jsonParser.getCodec().treeToValue(node.get("condition"), ConditionSpec.class);

        // 如果节点类型为 COMPOSITE，反序列化为 GraphSpec
        if (type.equals(NodeSpec.PatternNodeType.COMPOSITE)) {
            // 反序列化嵌套节点（nodes）
            List<NodeSpec> nodeSpecs = new ArrayList<>();
            Iterator<JsonNode> embeddedElementNames = node.get("nodes").elements();
            while (embeddedElementNames.hasNext()) {
                JsonNode jsonNode = embeddedElementNames.next();
                NodeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, NodeSpec.class);
                nodeSpecs.add(embedNode);
            }

            // 反序列化边（edges）
            List<EdgeSpec> edgeSpecs = new ArrayList<>();
            Iterator<JsonNode> jsonNodeIterator = node.get("edges").elements();
            while (jsonNodeIterator.hasNext()) {
                JsonNode jsonNode = jsonNodeIterator.next();
                EdgeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, EdgeSpec.class);
                edgeSpecs.add(embedNode);
            }

            // 反序列化窗口规格（window）
            WindowSpec window =
                    jsonParser.getCodec().treeToValue(node.get("window"), WindowSpec.class);

            // 反序列化匹配完成后的跳跃策略（afterMatchStrategy）
            AfterMatchSkipStrategySpec afterMatchStrategy =
                    jsonParser
                            .getCodec()
                            .treeToValue(
                                    node.get("afterMatchStrategy"),
                                    AfterMatchSkipStrategySpec.class);

            // 构造并返回 GraphSpec 对象
            return new GraphSpec(
                    name,
                    quantifierSpec,
                    conditionSpec,
                    nodeSpecs,
                    edgeSpecs,
                    window,
                    afterMatchStrategy);
        } else {
            // 如果节点类型不是 COMPOSITE，构造并返回普通节点（NodeSpec）
            return new NodeSpec(name, quantifierSpec, conditionSpec);
        }
    }

}
