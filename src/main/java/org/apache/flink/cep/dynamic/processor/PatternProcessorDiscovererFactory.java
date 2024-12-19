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

import java.io.Serializable;

/**
 * PatternProcessorDiscovererFactory 是一个工厂接口，用于创建 {@link PatternProcessorDiscoverer} 实例。
 *
 * <p>创建的 {@link PatternProcessorDiscoverer} 实例负责检测模式处理器的更新，并通知 {@link PatternProcessorManager}
 * 来处理模式处理器的变化。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public interface PatternProcessorDiscovererFactory<T> extends Serializable {
    /**
     * 创建一个 {@link PatternProcessorDiscoverer} 实例。
     *
     * <p>此方法允许通过提供用户代码类加载器（userCodeClassloader），动态创建用于发现模式处理器更新的实例。
     *
     * @param userCodeClassloader 用户代码类加载器，用于动态加载模式处理器相关的类和逻辑。
     * @return 一个 {@link PatternProcessorDiscoverer} 实例。
     * @throws Exception 如果创建失败，则抛出异常。
     */
    PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(ClassLoader userCodeClassloader)
            throws Exception;

}
