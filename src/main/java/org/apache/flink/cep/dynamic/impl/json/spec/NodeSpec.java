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

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.Quantifier.QuantifierProperty;
import org.apache.flink.cep.pattern.Quantifier.Times;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * NodeSpec 类用于描述模式（Pattern）并包含模式的所有必要字段。
 * 此类的主要功能是支持以 JSON 格式对 Node 进行序列化和反序列化。
 */

public class NodeSpec {
    // Node 的名称，标识该节点的唯一名称
    private final String name;

    // 量词规格，用于定义模式匹配的条件和属性
    private final QuantifierSpec quantifier;

    // 匹配条件，描述模式的触发逻辑
    private final ConditionSpec condition;

    // 节点的类型，可以是原子节点（ATOMIC）或组合节点（COMPOSITE）
    private final PatternNodeType type;


    /**
     * 构造一个 NodeSpec 对象。
     *
     * @param name 节点的名称。
     * @param quantifier 节点的量词规格，用于描述匹配条件。
     * @param condition 节点的匹配条件。
     */
    public NodeSpec(String name, QuantifierSpec quantifier, ConditionSpec condition) {
        // 默认构造的节点类型为原子节点（ATOMIC）
        this(name, quantifier, condition, PatternNodeType.ATOMIC);
    }

    /**
     * 构造一个 NodeSpec 对象。
     *
     * @param name 节点的名称。
     * @param quantifier 节点的量词规格，用于描述匹配条件。
     * @param condition 节点的匹配条件。
     * @param type 节点的类型，可以是原子节点或组合节点。
     */
    public NodeSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @JsonProperty("type") PatternNodeType type) {
        this.name = name;
        this.quantifier = quantifier;
        this.condition = condition;
        this.type = type;
    }


    /**
     * 从给定的 Pattern 构造一个 NodeSpec 对象。
     *
     * @param pattern 输入的模式对象。
     * @return 构造的 NodeSpec 对象。
     */
    public static NodeSpec fromPattern(Pattern<?, ?> pattern) {
        // 从 Pattern 中提取量词规格，包括匹配次数和条件
        QuantifierSpec quantifier = new QuantifierSpec(
                pattern.getQuantifier(), pattern.getTimes(), pattern.getUntilCondition());

        // 构造 NodeSpec 对象
        return new Builder()
                .name(pattern.getName()) // 设置节点名称
                .quantifier(quantifier) // 设置量词
                .condition(ConditionSpec.fromCondition(pattern.getCondition())) // 设置匹配条件
                .build();
    }


    /**
     * 将 NodeSpec 转换为 Pattern 对象。
     *
     * @param previous 上一个模式，表示当前模式的前驱。
     * @param afterMatchSkipStrategy 匹配完成后的跳跃策略。
     * @param consumingStrategy 消费策略，描述模式匹配的行为（严格、跳过等）。
     * @param classLoader 用于加载模式的类加载器。
     * @return 转换后的 Pattern 对象。
     * @throws Exception 在反序列化过程中可能抛出的异常。
     */
    public Pattern<?, ?> toPattern(
            final Pattern<?, ?> previous,
            final AfterMatchSkipStrategy afterMatchSkipStrategy,
            final ConsumingStrategy consumingStrategy,
            final ClassLoader classLoader)
            throws Exception {
        // 如果当前对象是 GraphSpec，则将其转换为对应的 Pattern
        if (this instanceof GraphSpec) {
            // TODO: 如果子图的 AfterMatchSkipStrategy 与主图不一致，应记录日志
            return ((GraphSpec) this).toPattern(classLoader);
        }

        // 构造一个新的 Pattern 对象
        Pattern<?, ?> pattern = new Pattern(
                this.getName(), previous, consumingStrategy, afterMatchSkipStrategy);

        // 处理匹配条件 TODO 匹配条件如何与avator建立关系？
        final ConditionSpec conditionSpec = this.getCondition();
        if (conditionSpec != null) {
            IterativeCondition iterativeCondition = conditionSpec.toIterativeCondition(classLoader);
            if (iterativeCondition instanceof RichOrCondition) {
                pattern.or(iterativeCondition); // 添加 "或" 条件
            } else {
                pattern.where(iterativeCondition); // 添加普通条件
            }
        }

        // 处理量词的属性
        for (QuantifierProperty property : this.getQuantifier().getProperties()) {
            if (property.equals(QuantifierProperty.OPTIONAL)) {
                pattern.optional(); // 设置为可选模式
            } else if (property.equals(QuantifierProperty.GREEDY)) {
                pattern.greedy(); // 设置为贪婪模式
            } else if (property.equals(QuantifierProperty.LOOPING)) {
                final Times times = this.getQuantifier().getTimes();
                if (times != null) {
                    pattern.timesOrMore(times.getFrom(), times.getWindowTime()); // 设置重复匹配次数
                }
            } else if (property.equals(QuantifierProperty.TIMES)) {
                final Times times = this.getQuantifier().getTimes();
                if (times != null) {
                    pattern.times(times.getFrom(), times.getTo()); // 设置精确匹配次数
                }
            }
        }

        // 处理量词的内部消费策略
        final ConsumingStrategy innerConsumingStrategy = this.getQuantifier().getConsumingStrategy();
        if (innerConsumingStrategy.equals(ConsumingStrategy.SKIP_TILL_ANY)) {
            pattern.allowCombinations(); // 允许组合
        } else if (innerConsumingStrategy.equals(ConsumingStrategy.STRICT)) {
            pattern.consecutive(); // 设置为连续匹配
        }

        // 处理 "直到" 条件
        final ConditionSpec untilCondition = this.getQuantifier().getUntilCondition();
        if (untilCondition != null) {
            final IterativeCondition iterativeCondition =
                    untilCondition.toIterativeCondition(classLoader);
            pattern.until(iterativeCondition); // 添加 "直到" 条件
        }

        return pattern;
    }


    /**
     * 获取节点名称。
     *
     * @return 节点的名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 获取节点类型。
     *
     * @return 节点的类型。
     */
    public PatternNodeType getType() {
        return type;
    }

    /**
     * 获取节点的量词规格。
     *
     * @return 节点的量词规格。
     */
    public QuantifierSpec getQuantifier() {
        return quantifier;
    }

    /**
     * 获取节点的匹配条件。
     *
     * @return 节点的匹配条件。
     */
    public ConditionSpec getCondition() {
        return condition;
    }


    /**
     * 节点的类型。
     */
    public enum PatternNodeType {
        // 原子节点（ATOMIC）是最基本的模式
        ATOMIC,
        // 组合节点（COMPOSITE）是一个子图（Graph）
        COMPOSITE
    }


    /**
     * NodeSpec 的 Builder 类，用于构造 NodeSpec 对象。
     */
    private static final class Builder {
        private String name; // 节点名称
        private QuantifierSpec quantifier; // 量词规格
        private ConditionSpec condition; // 匹配条件

        /**
         * 构造函数，初始化 Builder 对象。
         */
        private Builder() {}

        /**
         * 设置节点名称。
         *
         * @param name 节点名称。
         * @return Builder 本身，用于链式调用。
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * 设置量词规格。
         *
         * @param quantifier 量词规格。
         * @return Builder 本身，用于链式调用。
         */
        public Builder quantifier(QuantifierSpec quantifier) {
            this.quantifier = quantifier;
            return this;
        }

        /**
         * 设置匹配条件。
         *
         * @param condition 匹配条件。
         * @return Builder 本身，用于链式调用。
         */
        public Builder condition(ConditionSpec condition) {
            this.condition = condition;
            return this;
        }

        /**
         * 构造并返回 NodeSpec 对象。
         *
         * @return 构造的 NodeSpec 对象。
         */
        public NodeSpec build() {
            return new NodeSpec(this.name, this.quantifier, this.condition);
        }
    }

}
