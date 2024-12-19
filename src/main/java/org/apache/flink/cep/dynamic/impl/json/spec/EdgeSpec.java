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

package org.apache.flink.cep.dynamic.impl.json.spec;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * EdgeSpec 类用于描述两个节点（例如 {@link Pattern}）之间的事件选择策略（Event Selection Strategy）。
 *
 * 每个 Edge 对象连接两个节点，定义了事件在它们之间的传递方式。
 * 该类支持以 JSON 格式对边（Edge）进行序列化和反序列化。
 */

public class EdgeSpec {
    // 边的起始节点名称（源节点）
    private final String source;

    // 边的终止节点名称（目标节点）
    private final String target;

    // 边的事件消费策略（ConsumingStrategy），用于描述事件如何在两个节点之间流动
    private final ConsumingStrategy type;


    /**
     * 构造一个 EdgeSpec 对象。
     *
     * @param source 源节点的名称，表示事件流的起点。
     * @param target 目标节点的名称，表示事件流的终点。
     * @param type 事件消费策略，定义事件在两个节点之间的传递规则。
     */
    public EdgeSpec(
            @JsonProperty("source") String source,
            @JsonProperty("target") String target,
            @JsonProperty("type") ConsumingStrategy type) {
        this.source = source;
        this.target = target;
        this.type = type;
    }


    /**
     * 获取边的源节点名称。
     *
     * @return 源节点的名称。
     */
    public String getSource() {
        return source;
    }

    /**
     * 获取边的目标节点名称。
     *
     * @return 目标节点的名称。
     */
    public String getTarget() {
        return target;
    }

    /**
     * 获取边的事件消费策略。
     *
     * @return 事件消费策略（ConsumingStrategy）。
     */
    public ConsumingStrategy getType() {
        return type;
    }

}
