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
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SubTypeConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link SubtypeCondition} 进行序列化和反序列化。
 *
 * SubtypeCondition 是复杂事件处理（CEP）中的条件类，用于检查事件是否为某种子类型（Subclass）。
 *
 * 该类继承自 {@link ClassConditionSpec}，通过动态加载类的机制支持子类型的定义和匹配。
 */

public class SubTypeConditionSpec extends ClassConditionSpec {
    /**
     * 条件检查的子类型类名（Fully Qualified Class Name）。
     *
     * 用于定义和序列化条件中子类型的具体类。
     */
    private final String subClassName;


    /**
     * 构造一个 SubTypeConditionSpec 对象。
     *
     * @param className 条件类的全限定名称，用于序列化和反序列化时标识具体条件类。
     * @param subClassName 子类型类的全限定名称，用于定义子类型匹配逻辑。
     */
    public SubTypeConditionSpec(
            @JsonProperty("className") String className,
            @JsonProperty("subClassName") String subClassName) {
        // 调用父类构造函数，初始化条件类的全限定名称
        super(className);
        // 初始化子类型类名
        this.subClassName = subClassName;
    }

    /**
     * 从一个 IterativeCondition 对象构造 SubTypeConditionSpec。
     *
     * @param condition 输入的 SubtypeCondition 条件实例，用于提取条件类名和子类型类名。
     */
    public SubTypeConditionSpec(IterativeCondition condition) {
        // 调用父类构造函数，初始化条件类的全限定名称
        super(condition);
        // 从 SubtypeCondition 实例中提取子类型类名
        this.subClassName = ((SubtypeCondition) condition).getSubtype().getCanonicalName();
    }


    /**
     * 将当前 SubTypeConditionSpec 对象转换为 {@link SubtypeCondition} 对象。
     *
     * SubtypeCondition 是运行时使用的条件类，用于检查事件是否为指定子类型。
     *
     * @param classLoader 用于加载条件类和子类型类的类加载器。
     * @return 转换后的 {@link SubtypeCondition} 对象。
     * @throws Exception 在加载或实例化子类型类时可能抛出的异常。
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(ClassLoader classLoader) throws Exception {
        // 使用类加载器加载子类型类，并创建 SubtypeCondition 对象
        return new SubtypeCondition<>(classLoader.loadClass(this.getSubClassName()));
    }


    /**
     * 获取子类型类的全限定名称。
     *
     * @return 子类型类的全限定名称（Fully Qualified Class Name）。
     */
    public String getSubClassName() {
        return subClassName;
    }

}
