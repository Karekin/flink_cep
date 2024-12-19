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

package org.apache.flink.cep.dynamic.processor;

import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.io.Versioned;

import java.io.Serializable;

/**
 * PatternProcessor 是一个模式处理器的基础定义接口。
 *
 * <p>模式处理器定义了以下内容：
 * - 一个 {@link Pattern}，用于描述事件匹配的逻辑。
 * - 如何匹配该模式。
 * - 如何处理匹配到的结果。
 *
 * @param <IN> 表示出现在模式中的元素的基础类型。
 */

public interface PatternProcessor<IN> extends Serializable, Versioned {

    /**
     * 获取模式处理器的唯一标识符。
     *
     * @return 模式处理器的 ID。
     */
    String getId();


    /**
     * 获取模式处理器生效的时间戳。
     *
     * <p>模式处理器会在指定的时间戳后生效：
     * - 如果时间戳早于当前事件/处理时间，模式处理器会立即生效。
     * - 如果希望模式处理器始终立即生效，可以将时间戳设置为 {@code Long.MIN_VALUE}。
     *
     * @return 模式处理器的生效时间戳。
     */
    default Long getTimestamp() {
        return Long.MIN_VALUE;
    }


    /**
     * 获取要匹配的 {@link Pattern}。
     *
     * @param classLoader 用于加载模式的类加载器。
     * @return 与模式处理器相关联的模式。
     */
    Pattern<IN, ?> getPattern(ClassLoader classLoader);


    /**
     * 获取用于处理模式匹配结果的 {@link PatternProcessFunction}。
     *
     * @return 与模式处理器相关联的模式处理函数。
     */
    PatternProcessFunction<IN, ?> getPatternProcessFunction();

}
