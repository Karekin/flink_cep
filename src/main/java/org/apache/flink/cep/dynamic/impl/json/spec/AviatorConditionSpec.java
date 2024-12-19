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

import org.apache.flink.cep.dynamic.condition.AviatorCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * AviatorConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link AviatorCondition} 进行序列化和反序列化。
 *
 * AviatorCondition 是基于 Aviator 表达式的条件，用于定义 CEP（复杂事件处理）中的过滤逻辑。
 * 该类继承自 {@link ConditionSpec}，并实现了 Aviator 特定条件的处理逻辑。
 */

public class AviatorConditionSpec extends ConditionSpec {

    /**
     * 条件的过滤表达式。
     * 该表达式以 Aviator 脚本的形式定义，用于描述事件的过滤逻辑。
     */
    private final String expression;


    /**
     * 构造一个 AviatorConditionSpec 对象。
     *
     * @param expression 条件的过滤表达式，以 Aviator 脚本的形式提供。
     */
    public AviatorConditionSpec(@JsonProperty("expression") String expression) {
        // 调用父类构造函数，并将条件类型设置为 AVIATOR
        super(ConditionType.AVIATOR);
        this.expression = expression;
    }


    /**
     * 获取条件的过滤表达式。
     *
     * @return 条件的 Aviator 脚本表达式。
     */
    public String getExpression() {
        return expression;
    }


    /**
     * 将当前 AviatorConditionSpec 对象转换为 {@link AviatorCondition} 对象。
     *
     * AviatorCondition 是具体运行时使用的条件类，支持 Aviator 表达式的动态执行。
     *
     * @param classLoader 用于加载条件类的类加载器（在此方法中未使用）。
     * @return 转换后的 {@link AviatorCondition} 对象。
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(ClassLoader classLoader) {
        // 创建一个新的 AviatorCondition 对象，传入表达式
        return new AviatorCondition<>(expression);
    }

}
