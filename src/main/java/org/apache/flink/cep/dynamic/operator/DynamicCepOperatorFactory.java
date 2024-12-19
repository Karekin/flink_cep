/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.cep.dynamic.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.dynamic.coordinator.DynamicCepOperatorCoordinatorProvider;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link DynamicCepOperator} 的工厂类。
 *
 * <p>该类负责创建和配置动态复杂事件处理（CEP）的操作符实例，同时提供与 Flink 协调器集成的支持。
 *
 * <p>主要功能包括：
 * - 创建 {@link DynamicCepOperator} 实例。
 * - 提供协调器的工厂（Coordinator Provider）。
 * - 定义操作符的类类型。
 *
 * @param <IN> 输入事件的类型。
 * @param <OUT> 输出事件的类型。
 */

public class DynamicCepOperatorFactory<IN, OUT> extends AbstractStreamOperatorFactory<OUT>
        implements OneInputStreamOperatorFactory<IN, OUT>,
                CoordinatedOperatorFactory<OUT>,
                ProcessingTimeServiceAware {

    /** 用于动态发现模式处理器的工厂。 */
    private final PatternProcessorDiscovererFactory<IN> discovererFactory;

    /** 输入事件的序列化器，用于处理输入流的数据序列化和反序列化。 */
    private final TypeSerializer<IN> inputSerializer;

    /** 定义时间行为（事件时间或处理时间）。 */
    private final TimeBehaviour timeBehaviour;

    /** 序列化版本号，用于确保类的序列化和反序列化一致性。 */
    private static final long serialVersionUID = 1L;


    /**
     * 构造一个 {@link DynamicCepOperatorFactory} 实例。
     *
     * @param discovererFactory 模式处理器发现器的工厂，用于动态加载模式处理器。
     * @param inputSerializer 输入事件的序列化器。
     * @param timeBehaviour 定义时间行为（事件时间或处理时间）。
     */
    public DynamicCepOperatorFactory(
            final PatternProcessorDiscovererFactory<IN> discovererFactory,
            final TypeSerializer<IN> inputSerializer,
            final TimeBehaviour timeBehaviour) {
        this.discovererFactory = checkNotNull(discovererFactory, "发现器工厂不能为空");
        this.inputSerializer = checkNotNull(inputSerializer, "输入序列化器不能为空");
        this.timeBehaviour = timeBehaviour;
    }


    /**
     * 创建一个 {@link DynamicCepOperator} 实例。
     *
     * @param parameters 包含流操作符配置和上下文的参数。
     * @param <T> 流操作符的类型。
     * @return 创建的 {@link DynamicCepOperator} 实例。
     */
    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        // 获取操作符的 ID
        final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();

        // 创建动态 CEP 操作符实例
        final DynamicCepOperator<IN, ?, OUT> patternProcessorOperator =
                new DynamicCepOperator<>(inputSerializer, timeBehaviour == TimeBehaviour.ProcessingTime);

        // 设置操作符的运行时环境
        patternProcessorOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        // 配置处理时间服务
        patternProcessorOperator.setProcessingTimeService(parameters.getProcessingTimeService());

        // 注册操作符事件处理程序
        parameters
                .getOperatorEventDispatcher()
                .registerEventHandler(operatorId, patternProcessorOperator);

        // 将创建的操作符强制转换为泛型类型
        @SuppressWarnings("unchecked")
        final T castedOperator = (T) patternProcessorOperator;

        return castedOperator;
    }


    /**
     * 提供操作符协调器的工厂，TODO 实现操作符与 Flink 调度器的交互
     *
     * @param operatorName 操作符的名称。
     * @param operatorID 操作符的唯一标识符。
     * @return 一个新的 {@link DynamicCepOperatorCoordinatorProvider} 实例。
     */
    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {
        return new DynamicCepOperatorCoordinatorProvider<>(operatorName, operatorID, discovererFactory);
    }


    /**
     * 获取操作符的类类型。
     *
     * <p>此方法用于 Flink 系统识别该工厂生成的操作符类型。
     *
     * @param classLoader 类加载器。
     * @return {@link DynamicCepOperator} 的类类型。
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return DynamicCepOperator.class;
    }

}
