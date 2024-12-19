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

import java.io.Closeable;

/**
 * PatternProcessorDiscoverer 接口用于发现模式处理器的变更，通知 {@link PatternProcessorManager} 模式处理器的更新，
 * 并提供初始的模式处理器。
 *
 * <p>该接口的主要职责包括：
 * - 检测模式处理器的变更。
 * - 通知 {@link PatternProcessorManager} 以便更新管理的模式处理器集合。
 * - 在动态模式处理器变更场景中，提供一种持续监听的机制。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public interface PatternProcessorDiscoverer<T> extends Closeable {

    /**
     * 发现模式处理器的变更。
     *
     * <p>此方法在动态模式处理器变更的场景中应该是一个持续运行的过程，用于定期检查模式处理器的更新情况，
     * 并通知 {@link PatternProcessorManager} 以更新其管理的模式处理器集合。
     *
     * @param patternProcessorManager 模式处理器管理器，用于接收变更通知并更新管理的模式处理器集合。
     */
    void discoverPatternProcessorUpdates(PatternProcessorManager<T> patternProcessorManager);

}
