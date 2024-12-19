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
import org.apache.flink.cep.nfa.aftermatch.NoSkipStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipToFirstStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipToLastStrategy;
import org.apache.flink.cep.nfa.aftermatch.SkipToNextStrategy;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * AfterMatchSkipStrategySpec 类是一个工具类，用于以 JSON 格式对 {@link AfterMatchSkipStrategy} 进行序列化和反序列化。
 *
 * AfterMatchSkipStrategy 是复杂事件处理（CEP）中的一个关键策略，定义了在模式匹配完成后如何跳过事件。
 *
 * 此类通过枚举和映射，将策略的具体实现与其序列化表示联系起来，以支持动态加载和持久化。
 */

public class AfterMatchSkipStrategySpec {
    /**
     * 一个映射表，用于将具体策略类的全限定名称转换为枚举类型。
     *
     * 例如，将 NoSkipStrategy 类映射为 AfterMatchSkipStrategyType.NO_SKIP。
     */
    private static final Map<String, String> classToEnumTranslator = new HashMap<>();

    /**
     * 当前策略的类型。
     *
     * 使用枚举 {@link AfterMatchSkipStrategyType} 来标识策略的种类，例如 NO_SKIP、SKIP_TO_NEXT 等。
     */
    private final AfterMatchSkipStrategyType type;

    /**
     * 可选的模式名称，用于特定策略（例如 SKIP_TO_FIRST 和 SKIP_TO_LAST）时，指定需要跳转的模式。
     */
    private final @Nullable String patternName;


    /**
     * 初始化 classToEnumTranslator 映射表，将策略类的全限定名称与枚举类型关联。
     *
     * 该表用于在从 AfterMatchSkipStrategy 转换为 AfterMatchSkipStrategySpec 时，快速查找对应的枚举类型。
     */
    static {
        classToEnumTranslator.put(
                NoSkipStrategy.class.getCanonicalName(), AfterMatchSkipStrategyType.NO_SKIP.name());
        classToEnumTranslator.put(
                SkipToNextStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_NEXT.name());
        classToEnumTranslator.put(
                SkipPastLastStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_PAST_LAST_EVENT.name());
        classToEnumTranslator.put(
                SkipToFirstStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_FIRST.name());
        classToEnumTranslator.put(
                SkipToLastStrategy.class.getCanonicalName(),
                AfterMatchSkipStrategyType.SKIP_TO_LAST.name());
    }


    /**
     * 构造一个 AfterMatchSkipStrategySpec 对象。
     *
     * @param type 策略的类型，使用枚举 {@link AfterMatchSkipStrategyType} 表示。
     * @param patternName 可选的模式名称，用于特定策略时指定跳转的目标模式。
     */
    public AfterMatchSkipStrategySpec(
            @JsonProperty("type") AfterMatchSkipStrategyType type,
            @JsonProperty("patternName") @Nullable String patternName) {
        this.type = type;
        this.patternName = patternName;
    }


    /**
     * 获取当前策略的类型。
     *
     * @return 策略的类型（枚举 {@link AfterMatchSkipStrategyType}）。
     */
    public AfterMatchSkipStrategyType getType() {
        return type;
    }

    /**
     * 获取当前策略的模式名称（如果有）。
     *
     * @return 模式名称，如果未指定则返回 null。
     */
    @Nullable
    public String getPatternName() {
        return patternName;
    }


    /**
     * 将一个 {@link AfterMatchSkipStrategy} 转换为 AfterMatchSkipStrategySpec 对象。
     *
     * @param afterMatchSkipStrategy 输入的 AfterMatchSkipStrategy 对象。
     * @return 转换后的 AfterMatchSkipStrategySpec 对象。
     */
    public static AfterMatchSkipStrategySpec fromAfterMatchSkipStrategy(
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        // 根据策略的类名，在映射表中查找对应的枚举类型
        return new AfterMatchSkipStrategySpec(
                AfterMatchSkipStrategyType.valueOf(
                        classToEnumTranslator.get(
                                afterMatchSkipStrategy.getClass().getCanonicalName())),
                // 获取模式名称（如果存在）
                afterMatchSkipStrategy.getPatternName().orElse(null));
    }


    /**
     * 将当前 AfterMatchSkipStrategySpec 对象转换为具体的 {@link AfterMatchSkipStrategy} 实现。
     *
     * @return 转换后的 AfterMatchSkipStrategy 对象。
     * @throws IllegalStateException 如果策略类型无效或未实现，抛出异常。
     */
    public AfterMatchSkipStrategy toAfterMatchSkipStrategy() {
        switch (this.type) {
            case NO_SKIP:
                // 返回不跳过策略
                return NoSkipStrategy.noSkip();
            case SKIP_TO_LAST:
                // 返回跳到最后的策略，传入模式名称
                return SkipToLastStrategy.skipToLast(this.getPatternName());
            case SKIP_TO_NEXT:
                // 返回跳到下一个的策略
                return SkipToNextStrategy.skipToNext();
            case SKIP_TO_FIRST:
                // 返回跳到第一个的策略，传入模式名称
                return SkipToFirstStrategy.skipToFirst(this.getPatternName());
            case SKIP_PAST_LAST_EVENT:
                // 返回跳过最后一个事件的策略
                return SkipPastLastStrategy.skipPastLastEvent();
            default:
                // 如果类型无效，抛出异常
                throw new IllegalStateException(
                        "The type of the AfterMatchSkipStrategySpec: "
                                + this.type
                                + " is invalid!");
        }
    }


    /**
     * AfterMatchSkipStrategyType 枚举定义了所有支持的跳跃策略类型。
     *
     * 每种类型对应一个具体的策略类，用于序列化和反序列化时的标识。
     */
    public enum AfterMatchSkipStrategyType {
        // 不跳过任何事件
        NO_SKIP(NoSkipStrategy.class.getCanonicalName()),
        // 跳到下一个事件
        SKIP_TO_NEXT(SkipToNextStrategy.class.getCanonicalName()),
        // 跳过最后一个事件
        SKIP_PAST_LAST_EVENT(SkipPastLastStrategy.class.getCanonicalName()),
        // 跳到第一个事件
        SKIP_TO_FIRST(SkipToFirstStrategy.class.getCanonicalName()),
        // 跳到最后的事件
        SKIP_TO_LAST(SkipToLastStrategy.class.getCanonicalName());

        /**
         * 策略类的全限定名称（Fully Qualified Class Name）。
         */
        public final String className;

        /**
         * 构造函数，初始化枚举类型。
         *
         * @param className 策略类的全限定名称。
         */
        AfterMatchSkipStrategyType(String className) {
            this.className = className;
        }
    }

}
