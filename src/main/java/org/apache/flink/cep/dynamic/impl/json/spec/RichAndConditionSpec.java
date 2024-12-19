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
import org.apache.flink.cep.pattern.conditions.RichAndCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * RichAndConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link RichAndCondition} 进行序列化和反序列化。
 *
 * RichAndCondition 是复杂事件处理（CEP）中使用的条件类，用于将多个子条件组合成 "与" 逻辑。
 *
 * 该类继承自 {@link RichCompositeConditionSpec}，并实现了 RichAndCondition 特定的序列化与反序列化逻辑。
 */

public class RichAndConditionSpec extends RichCompositeConditionSpec {

    /**
     * 构造一个 RichAndConditionSpec 对象。
     *
     * @param nestedConditions 子条件的列表，每个子条件以 {@link ConditionSpec} 表示。
     *                         RichAndConditionSpec 要求必须有两个子条件。
     */
    public RichAndConditionSpec(
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造函数，指定条件类型为 RichAndCondition，并传递子条件列表
        super(RichAndCondition.class.getCanonicalName(), nestedConditions);
    }


    /**
     * 将当前 RichAndConditionSpec 对象转换为 {@link RichAndCondition} 对象。
     *
     * RichAndCondition 是运行时使用的条件类，表示多个子条件的 "与" 关系。
     *
     * @param classLoader 用于加载子条件类的类加载器。
     * @return 转换后的 {@link RichAndCondition} 对象。
     * @throws Exception 在加载或实例化子条件时可能抛出的异常。
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(ClassLoader classLoader) throws Exception {
        // 从子条件列表中加载第一个子条件，并转换为 IterativeCondition 对象
        IterativeCondition<?> firstCondition =
                this.getNestedConditions().get(0).toIterativeCondition(classLoader);

        // 从子条件列表中加载第二个子条件，并转换为 IterativeCondition 对象
        IterativeCondition<?> secondCondition =
                this.getNestedConditions().get(1).toIterativeCondition(classLoader);

        // 创建并返回 RichAndCondition 对象，将两个子条件组合成 "与" 逻辑
        return new RichAndCondition(firstCondition, secondCondition);
    }

}
