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
import org.apache.flink.cep.pattern.conditions.RichAndCondition;
import org.apache.flink.cep.pattern.conditions.RichCompositeIterativeCondition;
import org.apache.flink.cep.pattern.conditions.RichNotCondition;
import org.apache.flink.cep.pattern.conditions.RichOrCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * ConditionSpec 类是一个工具类，用于以 JSON 格式对特定类型的 {@link IterativeCondition} 进行序列化和反序列化。
 *
 * IterativeCondition 是 CEP（复杂事件处理）中的核心组件，用于定义模式的过滤条件。
 * 该类通过抽象和多态的方式支持多种条件类型的扩展和处理。
 */

public abstract class ConditionSpec {
    // 条件类型，用于标识条件的具体实现类型，例如 CLASS 或 AVIATOR
    private final ConditionType type;


    /**
     * 构造一个 ConditionSpec 对象。
     *
     * @param type 条件的类型，用于区分不同的条件实现。
     */
    ConditionSpec(@JsonProperty("type") ConditionType type) {
        this.type = type;
    }


    // TODO: rethink how to make adding a custom condition easier and cleaner
    /**
     * 将一个 IterativeCondition 转换为对应的 ConditionSpec 对象。
     *
     * 根据 IterativeCondition 的具体类型，创建相应的 ConditionSpec 子类对象。
     * 支持的类型包括 SubtypeCondition、AviatorCondition，以及多种组合条件（AND、OR、NOT）。
     *
     * @param condition 输入的 IterativeCondition 对象。
     * @return 转换后的 ConditionSpec 对象。
     */
    public static ConditionSpec fromCondition(IterativeCondition<?> condition) {
        if (condition instanceof SubtypeCondition) {
            // 如果是子类型条件，使用 SubTypeConditionSpec
            return new SubTypeConditionSpec(condition);
        } else if (condition instanceof AviatorCondition) {
            // 如果是 Aviator 条件，提取其表达式并创建 AviatorConditionSpec
            return new AviatorConditionSpec(((AviatorCondition<?>) condition).getExpression());
        } else if (condition instanceof RichCompositeIterativeCondition) {
            // 如果是组合条件，处理其嵌套条件
            IterativeCondition<?>[] nestedConditions =
                    ((RichCompositeIterativeCondition<?>) condition).getNestedConditions();
            if (condition instanceof RichOrCondition) {
                // OR 条件，递归转换嵌套条件为 ConditionSpec 列表 // TODO 嵌套转平铺？
                List<ConditionSpec> iterativeConditionSpecs = new ArrayList<>();
                for (IterativeCondition<?> iterativeCondition : nestedConditions) {
                    iterativeConditionSpecs.add(fromCondition(iterativeCondition));
                }
                return new RichOrConditionSpec(iterativeConditionSpecs);
            } else if (condition instanceof RichAndCondition) {
                // AND 条件，递归转换嵌套条件为 ConditionSpec 列表
                List<ConditionSpec> iterativeConditionSpecs = new ArrayList<>();
                for (IterativeCondition<?> iterativeCondition : nestedConditions) {
                    iterativeConditionSpecs.add(fromCondition(iterativeCondition));
                }
                return new RichAndConditionSpec(iterativeConditionSpecs);
            } else if (condition instanceof RichNotCondition) {
                // NOT 条件，递归转换嵌套条件为 ConditionSpec 列表
                List<ConditionSpec> iterativeConditionSpecs = new ArrayList<>();
                for (IterativeCondition<?> iterativeCondition : nestedConditions) {
                    iterativeConditionSpecs.add(fromCondition(iterativeCondition));
                }
                return new RichNotConditionSpec(iterativeConditionSpecs);
            }
        }
        // 默认返回 ClassConditionSpec
        return new ClassConditionSpec(condition);
    }


    /**
     * 将当前 ConditionSpec 对象转换为对应的 IterativeCondition 对象。
     *
     * 子类需要实现此方法，用于将序列化的 ConditionSpec 转换为实际运行时使用的 IterativeCondition。
     *
     * @param classLoader 用于加载条件类的类加载器。
     * @return 转换后的 IterativeCondition 对象。
     * @throws Exception 转换过程中可能抛出的异常。
     */
    public abstract IterativeCondition toIterativeCondition(ClassLoader classLoader) throws Exception;


    /**
     * 获取条件的类型。
     *
     * @return 条件类型（ConditionType）。
     */
    public ConditionType getType() {
        return type;
    }


    /**
     * ConditionType 枚举定义了条件的类型。
     *
     * 目前支持以下两种类型：
     * - CLASS：基于 Java 类的条件实现。
     * - AVIATOR：基于 Aviator 表达式的条件实现。
     */
    public enum ConditionType {
        // CLASS 类型条件
        CLASS,
        // AVIATOR 类型条件
        AVIATOR
    }

}
