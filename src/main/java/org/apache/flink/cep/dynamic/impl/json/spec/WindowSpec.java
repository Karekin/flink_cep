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
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * WindowSpec 类用于以 JSON 格式对 {@link Pattern} 的窗口时间（WindowTime）进行序列化和反序列化。
 *
 * 窗口时间定义了复杂事件处理（CEP）中模式匹配的时间范围。
 * 该类封装了窗口类型（WithinType）和时间长度（Time），以支持灵活的时间窗口配置。
 */

public class WindowSpec {
    /**
     * 窗口的类型，使用枚举 {@link WithinType} 表示。
     *
     * 常见的窗口类型包括：
     * - FIRST_AND_LAST：表示从第一个事件到最后一个事件的时间范围。
     * - PREVIOUS_AND_CURRENT：表示从前一个事件到当前事件的时间范围。
     */
    private final WithinType type;

    /**
     * 窗口的时间长度，使用 {@link Time} 对象表示。
     *
     * 该值定义了窗口的具体时间范围，例如 10 秒或 5 分钟。
     */
    private final Time time;


    /**
     * 构造一个 WindowSpec 对象。
     *
     * @param type 窗口的类型，定义事件匹配的时间范围。
     * @param time 窗口的时间长度。
     */
    public WindowSpec(@JsonProperty("type") WithinType type, @JsonProperty("time") Time time) {
        this.type = type;
        this.time = time;
    }


    /**
     * 从窗口时间的映射构造一个 WindowSpec 对象。
     *
     * @param window 窗口时间的映射，键为窗口类型，值为时间长度。
     *               该方法会优先选择 FIRST_AND_LAST 类型的时间，如果不存在则选择 PREVIOUS_AND_CURRENT 类型。
     * @return 构造的 WindowSpec 对象。
     */
    public static WindowSpec fromWindowTime(Map<WithinType, Time> window) {
        // 如果映射中包含 FIRST_AND_LAST 类型的时间，优先选择它
        if (window.containsKey(WithinType.FIRST_AND_LAST)) {
            return new WindowSpec(WithinType.FIRST_AND_LAST, window.get(WithinType.FIRST_AND_LAST));
        } else {
            // 否则选择 PREVIOUS_AND_CURRENT 类型的时间
            return new WindowSpec(
                    WithinType.PREVIOUS_AND_CURRENT, window.get(WithinType.FIRST_AND_LAST));
        }
    }


    /**
     * 获取窗口的时间长度。
     *
     * @return 窗口的时间长度（{@link Time} 对象）。
     */
    public Time getTime() {
        return time;
    }

    /**
     * 获取窗口的类型。
     *
     * @return 窗口的类型（枚举 {@link WithinType}）。
     */
    public WithinType getType() {
        return type;
    }

}
