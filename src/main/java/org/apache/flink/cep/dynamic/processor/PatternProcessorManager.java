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

import java.util.List;

/**
 * PatternProcessorManager 接口用于管理模式处理器，并处理模式处理器的更新通知。
 *
 * <p>该接口的主要职责包括：
 * - 监听并处理模式处理器的更新。
 * - 管理当前可用的模式处理器集合。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public interface PatternProcessorManager<T> {

    /**
     * 处理模式处理器更新的通知。 TODO 在分布式环境中，如何结合事件通知机制实现分布式同步？
     *
     * <p>当模式处理器的配置发生更新时，此方法会被调用。实现类应更新其内部存储的模式处理器集合，
     * 确保新的模式处理器配置能够正确生效。
     *
     * @param patternProcessors 已更新的模式处理器列表。
     */
    void onPatternProcessorsUpdated(List<PatternProcessor<T>> patternProcessors);

}
