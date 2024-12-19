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

import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.EnumSet;

/**
 * QuantifierSpec 类用于以 JSON 格式对 {@link Quantifier} 对象进行序列化和反序列化。
 * <p>
 * 此类包含了 Quantifier 的核心属性，以及附加的 {@code Times} 和 {@code untilCondition}，
 * 它们在逻辑上是 Quantifier 的一部分。
 */

public class QuantifierSpec {

    // 事件消费策略，定义了如何处理事件，例如严格顺序或允许跳过等
    private final ConsumingStrategy consumingStrategy;

    // 量词属性的集合，使用 EnumSet 存储多个属性，例如 OPTIONAL、GREEDY 等
    private final EnumSet<QuantifierProperty> properties;

    // 匹配次数的限制（Times），描述匹配的上下边界及窗口时间，可为空
    private final @Nullable Times times;

    // 直到条件（Until Condition），定义模式的终止条件，可为空
    private final @Nullable ConditionSpec untilCondition;


    /**
     * 构造一个 QuantifierSpec 对象，用于从 JSON 格式的输入还原对象。
     *
     * @param consumingStrategy 事件消费策略，定义如何处理事件。
     * @param properties        量词属性集合，用于标识量词的特性。
     * @param times             匹配次数限制，描述匹配的上下边界及窗口时间（可选）。
     * @param untilCondition    "直到" 条件，定义模式终止条件（可选）。
     */
    public QuantifierSpec(
            @JsonProperty("consumingStrategy") ConsumingStrategy consumingStrategy,
            @JsonProperty("properties") EnumSet<QuantifierProperty> properties,
            @Nullable @JsonProperty("times") Times times,
            @Nullable @JsonProperty("untilCondition") ConditionSpec untilCondition) {
        this.consumingStrategy = consumingStrategy;
        this.properties = properties;
        this.times = times;
        this.untilCondition = untilCondition;
    }


    /**
     * 从一个 Quantifier 对象构造 QuantifierSpec 对象。
     *
     * @param quantifier     原始的 Quantifier 对象。
     * @param times          匹配次数限制，用于描述匹配的上下边界及窗口时间（可选）。
     * @param untilCondition "直到" 条件，定义模式终止条件（可选）。
     */
    public QuantifierSpec(Quantifier quantifier, Times times, IterativeCondition untilCondition) {
        // 获取 Quantifier 的内部消费策略
        this.consumingStrategy = quantifier.getInnerConsumingStrategy();

        // 初始化量词属性集合，遍历 QuantifierProperty 的枚举值，并添加存在的属性
        this.properties = EnumSet.noneOf(QuantifierProperty.class);
        for (QuantifierProperty property : QuantifierProperty.values()) {
            if (quantifier.hasProperty(property)) {
                this.properties.add(property);
            }
        }

        // 初始化匹配次数限制，如果 times 不为空，则复制其值
        this.times =
                times == null
                        ? null
                        : Times.of(times.getFrom(), times.getTo(), times.getWindowTime());

        // 初始化 "直到" 条件，如果 untilCondition 不为空，则转换为 ClassConditionSpec
        this.untilCondition =
                untilCondition == null ? null : new ClassConditionSpec(untilCondition);
    }


    /**
     * 获取事件消费策略。
     *
     * @return 事件消费策略（ConsumingStrategy）。
     */
    public ConsumingStrategy getConsumingStrategy() {
        return consumingStrategy;
    }

    /**
     * 获取量词属性集合。
     *
     * @return 包含量词属性的 EnumSet 对象。
     */
    public EnumSet<QuantifierProperty> getProperties() {
        return properties;
    }

    /**
     * 获取匹配次数限制。
     *
     * @return 匹配次数限制（Times），如果未定义则返回 null。
     */
    @Nullable
    public Times getTimes() {
        return times;
    }

    /**
     * 获取 "直到" 条件。
     *
     * @return "直到" 条件（ConditionSpec），如果未定义则返回 null。
     */
    @Nullable
    public ConditionSpec getUntilCondition() {
        return untilCondition;
    }

}
