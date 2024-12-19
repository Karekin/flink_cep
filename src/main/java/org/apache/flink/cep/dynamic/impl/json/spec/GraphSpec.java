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
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * GraphSpec 类用于描述复杂的模式（Pattern），该模式包含节点（Node，例如 {@link Pattern}）和边（Edge）。
 * GraphSpec 的节点可以是嵌套的 Graph，从而支持复杂的模式组合。
 * 此类的主要功能是支持以 JSON 格式对 Graph 进行序列化和反序列化。
 */

public class GraphSpec extends NodeSpec {
    // Graph 的版本号，默认值为 1
    private final int version = 1;

    // Graph 的节点列表，每个节点代表模式的一部分
    private final List<NodeSpec> nodes;

    // Graph 的边列表，每条边定义了两个节点之间的关系
    private final List<EdgeSpec> edges;

    // Graph 的时间窗口，定义事件匹配的时间范围
    private final WindowSpec window;

    // Graph 的跳跃策略，描述模式匹配完成后的行为
    private final AfterMatchSkipStrategySpec afterMatchStrategy;


    /**
     * 构造一个 GraphSpec 对象。
     *
     * @param name               图的名称，用于标识该 Graph。
     * @param quantifier         量词，用于定义模式的匹配条件。
     * @param condition          匹配的条件，用于进一步限制匹配逻辑。
     * @param nodes              图中的节点列表，每个节点可以是一个简单节点或嵌套的 Graph。
     * @param edges              图中的边列表，定义了节点之间的连接关系。
     * @param window             时间窗口规格，用于限制匹配的时间范围。
     * @param afterMatchStrategy 跳跃策略，定义匹配完成后的处理方式。
     */
    public GraphSpec(
            @JsonProperty("name") String name,
            @JsonProperty("quantifier") QuantifierSpec quantifier,
            @JsonProperty("condition") ConditionSpec condition,
            @JsonProperty("nodes") List<NodeSpec> nodes,
            @JsonProperty("edges") List<EdgeSpec> edges,
            @JsonProperty("window") WindowSpec window,
            @JsonProperty("afterMatchStrategy") AfterMatchSkipStrategySpec afterMatchStrategy) {
        // 调用父类构造函数并设置节点类型为组合节点
        super(name, quantifier, condition, PatternNodeType.COMPOSITE);
        this.nodes = nodes;
        this.edges = edges;
        this.window = window;
        this.afterMatchStrategy = afterMatchStrategy;
    }


    /**
     * 从一个 Pattern 对象构造 GraphSpec。
     *
     * @param pattern 输入的模式对象，用于生成 GraphSpec。
     * @return 构造的 GraphSpec 对象。
     */
    public static GraphSpec fromPattern(Pattern<?, ?> pattern) {
        // 提取 GraphSpec 的元数据，例如名称、量词、时间窗口等

        // 提取名称，根据是否为 GroupPattern 决定名称来源
        String name = pattern instanceof GroupPattern
                ? ((GroupPattern<?, ?>) pattern).getRawPattern().getName()
                : pattern.getName();

        // 提取量词，包含匹配条件和次数限制
        QuantifierSpec quantifier = new QuantifierSpec(
                pattern.getQuantifier(), pattern.getTimes(), pattern.getUntilCondition());

        // 构造时间窗口规格 TODO 和Json案例对不上
        Map<WithinType, Time> window = new HashMap<>();
        if (pattern.getWindowTime(WithinType.FIRST_AND_LAST) != null) {
            window.put(WithinType.FIRST_AND_LAST, pattern.getWindowTime(WithinType.FIRST_AND_LAST));
        } else if (pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT) != null) {
            window.put(
                    WithinType.PREVIOUS_AND_CURRENT,
                    pattern.getWindowTime(WithinType.PREVIOUS_AND_CURRENT));
        }

        // 构造 GraphSpec 的 Builder，用于后续补充节点和边信息
        Builder builder = new Builder()
                .name(name)
                .quantifier(quantifier)
                .afterMatchStrategy(pattern.getAfterMatchSkipStrategy());

        // 如果存在时间窗口，添加到 Builder 中
        if (window.size() > 0) {
            builder.window(WindowSpec.fromWindowTime(window));
        }

        // 递归构造嵌套模式的节点和边
        List<NodeSpec> nodes = new ArrayList<>();
        List<EdgeSpec> edges = new ArrayList<>();
        while (pattern != null) {
            if (pattern instanceof GroupPattern) {
                // 处理子图（嵌套 Graph）模式
                GraphSpec subgraphSpec =
                        GraphSpec.fromPattern(((GroupPattern<?, ?>) pattern).getRawPattern());
                nodes.add(subgraphSpec);
            } else {
                // 构造简单节点
                NodeSpec nodeSpec = NodeSpec.fromPattern(pattern);
                nodes.add(nodeSpec);
            }

            // 如果存在前置节点，创建边信息
            if (pattern.getPrevious() != null) {
                edges.add(
                        new EdgeSpec(
                                pattern.getPrevious() instanceof GroupPattern
                                        ? ((GroupPattern<?, ?>) pattern.getPrevious())
                                        .getRawPattern()
                                        .getName()
                                        : pattern.getPrevious().getName(),
                                pattern instanceof GroupPattern
                                        ? ((GroupPattern<?, ?>) pattern).getRawPattern().getName()
                                        : pattern.getName(),
                                pattern.getQuantifier().getConsumingStrategy()));
            }

            // 更新为前一个模式，继续循环
            pattern = pattern.getPrevious();
        }

        // 将节点和边添加到 Builder，并构造最终的 GraphSpec 对象
        builder.nodes(nodes).edges(edges);
        return builder.build();
    }


    /**
     * 将 GraphSpec 转换为 Pattern 对象。
     *
     * @param classLoader 用于加载 Pattern 的类加载器。
     * @return 转换后的 Pattern 对象。
     * @throws Exception 在反序列化过程中可能抛出的异常。
     */
    public Pattern<?, ?> toPattern(final ClassLoader classLoader) throws Exception {
        // 构建节点和边的缓存，用于快速查找
        final Map<String, NodeSpec> nodeCache = new HashMap<>();
        for (NodeSpec node : nodes) {
            nodeCache.put(node.getName(), node);
        }
        final Map<String, EdgeSpec> edgeCache = new HashMap<>();
        for (EdgeSpec edgeSpec : edges) {
            edgeCache.put(edgeSpec.getSource(), edgeSpec);
        }

        // 构造 Pattern 序列
        String currentNodeName = findBeginPatternName();
        Pattern<?, ?> prevPattern = null;
        String prevNodeName = null; // 第一个节点的前一环节总是为null
        while (currentNodeName != null) {
            // 获取当前节点的规格
            NodeSpec currentNodeSpec = nodeCache.get(currentNodeName);
            EdgeSpec edgeToCurrentNode = edgeCache.get(prevNodeName);

            // 构造原子模式（Atomic Pattern），并继承 Graph 的跳跃策略
            Pattern<?, ?> currentPattern =
                    currentNodeSpec.toPattern(
                            prevPattern,
                            afterMatchStrategy.toAfterMatchSkipStrategy(),
                            prevNodeName == null
                                    ? ConsumingStrategy.STRICT
                                    : edgeToCurrentNode.getType(),
                            classLoader);

            // 如果是分组模式，进一步包装 TODO 什么是分组模式？更复杂的Json长什么样子？
            if (currentNodeSpec instanceof GraphSpec) {
                ConsumingStrategy strategy = prevNodeName == null
                        ? ConsumingStrategy.STRICT
                        : edgeToCurrentNode.getType();
                prevPattern = buildGroupPattern(strategy, currentPattern, prevPattern, prevNodeName == null);
            } else {
                prevPattern = currentPattern;
            }

            // 更新为下一个节点
            prevNodeName = currentNodeName;
            currentNodeName =
                    edgeCache.get(currentNodeName) == null
                            ? null
                            : edgeCache.get(currentNodeName).getTarget();
        }

        // 添加时间窗口语义
        if (window != null && prevPattern != null) {
            prevPattern.within(this.window.getTime(), this.window.getType());
        }

        return prevPattern;
    }


    public int getVersion() {
        return version;
    }

    public List<NodeSpec> getNodes() {
        return nodes;
    }

    public List<EdgeSpec> getEdges() {
        return edges;
    }

    public WindowSpec getWindow() {
        return window;
    }

    public AfterMatchSkipStrategySpec getAfterMatchStrategy() {
        return afterMatchStrategy;
    }

    /**
     * 查找 Graph 中的起始节点名称。
     * <p>
     * 起始节点是图中唯一一个没有被其他节点指向的节点。
     *
     * @return 起始节点的名称。
     * @throws IllegalStateException 如果没有找到唯一的起始节点，抛出异常。
     */
    private String findBeginPatternName() {
        // 用于存储所有节点的名称
        final Set<String> nodeSpecSet = new HashSet<>();
        for (NodeSpec node : nodes) {
            nodeSpecSet.add(node.getName());
        }

        // 从节点集合中移除所有被其他节点指向的节点（即边的目标节点）
        for (EdgeSpec edgeSpec : edges) {
            nodeSpecSet.remove(edgeSpec.getTarget());
        }

        // 如果剩余的节点数量不是 1，则抛出异常，说明起始节点不唯一或不存在
        if (nodeSpecSet.size() != 1) {
            throw new IllegalStateException(
                    "There must be exactly one begin node, but there are "
                            + nodeSpecSet.size()
                            + " nodes that are not pointed by any other nodes.");
        }

        // 返回唯一的起始节点名称
        Iterator<String> iterator = nodeSpecSet.iterator();
        if (!iterator.hasNext()) {
            throw new RuntimeException("Could not find the begin node.");
        }

        return iterator.next();
    }


    /**
     * 构造分组模式（GroupPattern）。
     *
     * 根据消费策略（ConsumingStrategy），将当前节点和前一个节点组装成一个分组模式。
     *
     * @param strategy 消费策略，定义模式之间的匹配行为（严格、跳过等）。
     * @param currentPattern 当前节点对应的模式。
     * @param prevPattern 前一个节点对应的模式。
     * @param isBeginPattern 是否是起始模式。
     * @return 构造的分组模式对象。
     */
    private GroupPattern<?, ?> buildGroupPattern(
            ConsumingStrategy strategy,
            Pattern<?, ?> currentPattern,
            Pattern<?, ?> prevPattern,
            boolean isBeginPattern) {
        // 根据不同的消费策略构建分组模式
        if (strategy.equals(ConsumingStrategy.STRICT)) {
            if (isBeginPattern) {
                // 如果是起始模式，设置为模式的起点
                currentPattern = Pattern.begin(currentPattern);
            } else {
                // 否则将当前模式链接到前一个模式
                currentPattern = prevPattern.next((Pattern) currentPattern);
            }
        } else if (strategy.equals(ConsumingStrategy.SKIP_TILL_NEXT)) {
            // 如果消费策略为 "跳过到下一个"，将当前模式标记为紧跟前一个模式
            currentPattern = prevPattern.followedBy((Pattern) currentPattern);
        } else if (strategy.equals(ConsumingStrategy.SKIP_TILL_ANY)) {
            // 如果消费策略为 "跳过到任意"，将当前模式标记为任意顺序的后续模式
            currentPattern = prevPattern.followedByAny((Pattern) currentPattern);
        }

        // 返回构造好的分组模式
        return (GroupPattern<?, ?>) currentPattern;
    }


    /**
     * GraphSpec 的 Builder 类，用于构造 GraphSpec 对象。
     */
    private static final class Builder {
        private String name; // 图的名称
        private QuantifierSpec quantifier; // 量词规格，用于定义匹配条件
        private List<NodeSpec> nodes; // 节点列表
        private List<EdgeSpec> edges; // 边列表
        private WindowSpec window; // 时间窗口规格
        private AfterMatchSkipStrategySpec afterMatchStrategy; // 匹配完成后的跳跃策略

        /**
         * 构造函数，初始化 Builder 对象。
         */
        private Builder() {}

        /**
         * 设置 Graph 的节点。
         *
         * @param nodes 节点列表。
         * @return Builder 本身，用于链式调用。
         */
        public Builder nodes(List<NodeSpec> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * 设置 Graph 的边。
         *
         * @param edges 边列表。
         * @return Builder 本身，用于链式调用。
         */
        public Builder edges(List<EdgeSpec> edges) {
            this.edges = edges;
            return this;
        }

        /**
         * 设置 Graph 的时间窗口规格。
         *
         * @param window 时间窗口规格。
         * @return Builder 本身，用于链式调用。
         */
        public Builder window(WindowSpec window) {
            this.window = window;
            return this;
        }

        /**
         * 设置匹配完成后的跳跃策略。
         *
         * @param afterMatchStrategy 跳跃策略。
         * @return Builder 本身，用于链式调用。
         */
        public Builder afterMatchStrategy(AfterMatchSkipStrategy afterMatchStrategy) {
            this.afterMatchStrategy =
                    AfterMatchSkipStrategySpec.fromAfterMatchSkipStrategy(afterMatchStrategy);
            return this;
        }

        /**
         * 设置 Graph 的名称。
         *
         * @param name 图的名称。
         * @return Builder 本身，用于链式调用。
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * 设置 Graph 的量词。
         *
         * @param quantifier 量词规格。
         * @return Builder 本身，用于链式调用。
         */
        public Builder quantifier(QuantifierSpec quantifier) {
            this.quantifier = quantifier;
            return this;
        }

        /**
         * 构造并返回 GraphSpec 对象。
         *
         * @return 构造的 GraphSpec 对象。
         */
        public GraphSpec build() {
            return new GraphSpec(name, quantifier, null, nodes, edges, window, afterMatchStrategy);
        }
    }

}
