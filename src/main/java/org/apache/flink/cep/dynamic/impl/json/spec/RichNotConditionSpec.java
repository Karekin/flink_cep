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

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichNotCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * RichNotConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link RichNotCondition} 进行序列化和反序列化。
 *
 * RichNotCondition 是复杂事件处理（CEP）中的条件类，用于对单个子条件应用 "非"（NOT）逻辑。
 *
 * 该类继承自 {@link RichCompositeConditionSpec}，支持动态加载条件类，并管理一个嵌套的子条件。
 */

public class RichNotConditionSpec extends RichCompositeConditionSpec {
    /**
     * 构造一个 RichNotConditionSpec 对象。
     *
     * @param nestedConditions 嵌套条件的列表，表示 "非"（NOT）逻辑应用的子条件。
     *                         该列表要求必须只包含一个子条件。
     */
    public RichNotConditionSpec(
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造函数，指定条件类型为 RichNotCondition，并传递子条件列表
        super(RichNotCondition.class.getCanonicalName(), nestedConditions);
    }

    /**
     * 将当前 RichNotConditionSpec 对象转换为 {@link RichNotCondition} 对象。
     *
     * RichNotCondition 是运行时使用的条件类，用于对一个子条件应用 "非"（NOT）逻辑。
     *
     * @param classLoader 用于加载子条件类的类加载器。
     * @return 转换后的 {@link RichNotCondition} 对象。
     * @throws Exception 在加载或实例化子条件时可能抛出的异常。
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(ClassLoader classLoader) throws Exception {
        // 从嵌套条件列表中加载第一个子条件，并转换为 IterativeCondition 对象
        IterativeCondition<?> innerCondition =
                this.getNestedConditions().get(0).toIterativeCondition(classLoader);

        // 创建并返回 RichNotCondition 对象，将子条件包装为 "非" 逻辑
        return new RichNotCondition(innerCondition);
    }

}
