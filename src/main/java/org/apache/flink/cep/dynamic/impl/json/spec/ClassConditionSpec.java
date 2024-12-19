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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ClassConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link IterativeCondition} 进行序列化和反序列化。
 *
 * 此类主要针对基于类的条件，将条件类的全限定名称（Class Name）作为标识符进行存储和加载。
 *
 * 使用场景：
 * - 动态模式匹配中，条件类的定义和加载需要通过类名反射完成。
 * - 支持条件逻辑的持久化和跨环境加载。
 */

public class ClassConditionSpec extends ConditionSpec {
    /**
     * 条件类的全限定名称（Fully Qualified Class Name）。
     *
     * 该名称用于反射加载条件类，实例化为 {@link IterativeCondition} 对象。
     */
    private final String className;


    /**
     * 构造一个 ClassConditionSpec 对象。
     *
     * @param className 条件类的全限定名称，用于序列化和反序列化时标识具体条件类。
     */
    public ClassConditionSpec(@JsonProperty("className") String className) {
        // 调用父类构造函数，并设置条件类型为 CLASS
        super(ConditionType.CLASS);
        this.className = className;
    }


    /**
     * 从给定的 {@link IterativeCondition} 对象构造一个 ClassConditionSpec 对象。
     *
     * @param condition 条件实例，提取其类的全限定名称用于存储。
     */
    public ClassConditionSpec(IterativeCondition<?> condition) {
        // 调用父类构造函数，并设置条件类型为 CLASS
        super(ConditionType.CLASS);
        // 获取条件类的全限定名称
        this.className = condition.getClass().getCanonicalName();
    }


    /**
     * 将当前 ClassConditionSpec 对象转换为 {@link IterativeCondition} 对象。
     *
     * 通过类加载器反射加载指定的条件类，并实例化为 {@link IterativeCondition}。
     * 如果类名为 "null" 或是本地/匿名类，则抛出异常。
     *
     * @param classLoader 用于加载条件类的类加载器。
     * @return 转换后的 {@link IterativeCondition} 对象。
     * @throws Exception 在加载或实例化类时可能抛出的异常。
     */
    @Override
    public IterativeCondition<?> toIterativeCondition(ClassLoader classLoader) throws Exception {
        // 如果类名为 "null"，抛出异常，提示不支持匿名类或本地类
        if (this.className.equals("null")) {
            throw new IllegalAccessException(
                    "It is not supported to save/load a local or anonymous class as a IterativeCondition for dynamic patterns.");
        }

        // 使用类加载器反射加载条件类，并实例化为 IterativeCondition 对象
        return (IterativeCondition<?>) classLoader.loadClass(this.getClassName()).newInstance();
    }


    /**
     * 获取条件类的全限定名称。
     *
     * @return 条件类的全限定名称（Fully Qualified Class Name）。
     */
    public String getClassName() {
        return className;
    }

}
