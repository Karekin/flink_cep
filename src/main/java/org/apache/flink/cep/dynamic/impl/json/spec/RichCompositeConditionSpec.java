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

import org.apache.flink.cep.pattern.conditions.RichCompositeIterativeCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * RichCompositeConditionSpec 类是一个工具类，用于以 JSON 格式对 {@link RichCompositeIterativeCondition} 进行序列化和反序列化。
 *
 * RichCompositeIterativeCondition 是复杂事件处理（CEP）中的组合条件类，用于将多个子条件组合在一起。
 * 该类通过继承 {@link ClassConditionSpec}，支持动态加载条件类，并额外管理嵌套子条件。
 */

public class RichCompositeConditionSpec extends ClassConditionSpec {
    /**
     * 嵌套条件的列表，每个子条件以 {@link ConditionSpec} 表示。
     *
     * 这些条件可以是简单条件（如 AviatorCondition）或其他复合条件（如 RichAndCondition）。
     */
    private final List<ConditionSpec> nestedConditions;


    /**
     * 构造一个 RichCompositeConditionSpec 对象。
     *
     * @param className 条件类的全限定名称，用于序列化和反序列化时标识具体条件类。
     * @param nestedConditions 嵌套条件的列表，表示组合条件中的所有子条件。
     */
    public RichCompositeConditionSpec(
            @JsonProperty("className") String className,
            @JsonProperty("nestedConditions") List<ConditionSpec> nestedConditions) {
        // 调用父类构造函数，初始化类名
        super(className);
        // 初始化嵌套条件列表，确保列表的独立性
        this.nestedConditions = new ArrayList<>(nestedConditions);
    }


    /**
     * 获取嵌套条件的列表。
     *
     * @return 嵌套条件列表，每个子条件以 {@link ConditionSpec} 表示。
     */
    public List<ConditionSpec> getNestedConditions() {
        return nestedConditions;
    }

}
