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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.dynamic.operator.DynamicCepOperatorFactory;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * 复杂事件处理（CEP）的工具类。
 *
 * <p>提供了一组静态方法，将 {@link DataStream} 转换为 {@link PatternStream}，
 * 以便进行复杂事件处理模式的检测和处理。
 *
 * <p>主要功能包括：
 * - 创建基于模式的 {@link PatternStream}。
 * - 支持动态模式处理，生成包含模式检测结果的 {@link DataStream}。
 */

public class CEP {
    /**
     * 从输入的 {@link DataStream} 和指定的模式创建一个 {@link PatternStream}。
     *
     * @param input 包含输入事件的 {@link DataStream}。
     * @param pattern 要检测的模式规格 {@link Pattern}。
     * @param <T> 输入事件的类型。
     * @return 包含模式检测结果的 {@link PatternStream}。
     */
    public static <T> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
        return new PatternStream<>(input, pattern);
    }


    /**
     * 从输入的 {@link DataStream} 和指定的模式创建一个 {@link PatternStream}，支持事件排序。
     *
     * <p>此方法提供了对具有相同时间戳的事件进行排序的能力。
     *
     * @param input 包含输入事件的 {@link DataStream}。
     * @param pattern 要检测的模式规格 {@link Pattern}。
     * @param comparator 用于对具有相同时间戳的事件进行排序的比较器。
     * @param <T> 输入事件的类型。
     * @return 包含模式检测结果的 {@link PatternStream}。
     */
    public static <T> PatternStream<T> pattern(
            DataStream<T> input, Pattern<T, ?> pattern, EventComparator<T> comparator) {
        final PatternStream<T> stream = new PatternStream<>(input, pattern);
        return stream.withComparator(comparator);
    }


    /**
     * 基于动态模式创建包含模式检测结果的 {@link DataStream}。
     *
     * <p>此方法支持动态模式处理功能，通过 {@link PatternProcessorDiscoverer} 动态发现模式处理器。
     *
     * @param input 包含输入事件的 {@link DataStream}。
     * @param patternProcessorDiscovererFactory 用于创建 {@link PatternProcessorDiscoverer} 的工厂。
     * @param timeBehaviour 定义时间依赖操作的时间行为（事件时间或处理时间）。
     * @param outTypeInfo 输出元素类型的显式说明。
     * @param <T> 输入事件的类型。
     * @param <R> 输出元素的类型。
     * @return 包含模式检测结果的 {@link DataStream}。
     */
    public static <T, R> SingleOutputStreamOperator<R> dynamicPatterns(
            DataStream<T> input,
            PatternProcessorDiscovererFactory<T> patternProcessorDiscovererFactory,
            TimeBehaviour timeBehaviour,
            TypeInformation<R> outTypeInfo) {

        // 创建动态模式处理器工厂
        final DynamicCepOperatorFactory<T, R> operatorFactory =
                new DynamicCepOperatorFactory<>(
                        patternProcessorDiscovererFactory,
                        input.getType().createSerializer(input.getExecutionConfig()),
                        timeBehaviour);

        /*
            如果输入流是键控流，使用 KeyedStream 的 transform 方法

            这两个名称 PatternProcessorOperator 和 GlobalPatternProcessorOperator 可能只是逻辑上的标识符，
            用于在 transform 方法中设置操作符的名称，并不是具体的类名或算子实现。
            在 Flink 中，transform 方法允许自定义操作符并指定一个名称来描述操作符的功能。
            名称仅用于日志、Web UI 和调试中标识算子，而不需要与具体的类或算子实现直接对应。
         */
        if (input instanceof KeyedStream) {
            KeyedStream<T, ?> keyedStream = (KeyedStream<T, ?>) input;
            /*
                这里的 "PatternProcessorOperator" 是操作符的名称，
                而 operatorFactory 是一个 DynamicCepOperatorFactory 实例，用于创建真正的底层算子。

                DynamicCepOperatorFactory 中会生成具体的算子逻辑，
                例如一个扩展了 AbstractStreamOperator 或实现了 StreamOperator 接口的类。
                这是 Flink 中自定义算子的标准实现方式。
             */
            return keyedStream.transform("PatternProcessorOperator", outTypeInfo, operatorFactory);
        } else {
            // 如果输入流不是键控流，使用 keyBy 方法将其转换为全局流并强制为单线程
            return input.keyBy(new NullByteKeySelector<>())
                    .transform("GlobalPatternProcessorOperator", outTypeInfo, operatorFactory)
                    .forceNonParallel();
        }
    }

}
