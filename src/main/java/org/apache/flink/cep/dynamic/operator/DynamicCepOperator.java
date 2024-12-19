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

package org.apache.flink.cep.dynamic.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.event.UpdatePatternProcessorEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.cep.operator.CepRuntimeContext;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Stream;

import static org.apache.flink.cep.operator.CepOperator.PATTERN_MATCHED_TIMES_METRIC_NAME;
import static org.apache.flink.cep.operator.CepOperator.PATTERN_MATCHING_AVG_TIME_METRIC_NAME;

/**
 * 基于键控输入流的模式处理操作符。
 *
 * <p>对于每个键，该操作符会创建多个 {@link NFA}（非确定性有限状态机）和一个优先队列，用于缓冲乱序到达的事件。
 * 这些数据结构使用托管的键控状态存储。
 *
 * <p>主要功能：
 * - 管理模式处理器和状态机（NFA）的初始化和生命周期。
 * - 支持事件时间和处理时间的 CEP（复杂事件处理）。
 * - 实现事件处理逻辑，包括乱序处理和延迟事件的丢弃。
 * - 提供模式匹配的度量指标。
 *
 * @param <IN> 输入事件的类型。
 * @param <KEY> 键的类型，用于键控输入流。
 * @param <OUT> 输出事件的类型。
 */
@Internal
public class DynamicCepOperator<IN, KEY, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>,
                Triggerable<KEY, VoidNamespace>,
                OperatorEventHandler {
    /** 日志记录器，用于记录操作符的运行时信息。 */
    private static final Logger LOG = LoggerFactory.getLogger(DynamicCepOperator.class);

    /** 序列化版本号，用于确保类的兼容性。 */
    private static final long serialVersionUID = 1L;

    /** 延迟事件丢弃的度量指标名称。 */
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    /** NFA（非确定性有限状态机）状态的名称。 */
    private static final String NFA_STATE_NAME = "nfaStateName";

    /** 事件队列状态的名称。 */
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

    ///////////////			State			//////////////

    /** 是否使用处理时间作为时间语义。 */
    private final boolean isProcessingTime;

    /** 输入事件的序列化器，用于管理输入流的数据序列化。 */
    private final TypeSerializer<IN> inputSerializer;

    /** 用于管理操作符状态初始化的上下文。 */
    private transient StateInitializationContext initializationContext;

    /** 用于存储 NFA 的状态集合，每个模式对应一个 NFA 状态。 */
    private transient Map<NFA<IN>, ValueState<NFAState>> computationStates;

    /** 用于存储乱序事件队列，每个 NFA 对应一个事件队列状态。 */
    private transient Map<NFA<IN>, MapState<Long, List<IN>>> elementQueueStates;

    /** 用于存储部分匹配结果，每个 NFA 对应一个共享缓冲区。 */
    private transient Map<NFA<IN>, SharedBuffer<IN>> partialMatches;

    /** 管理定时器服务，每个 NFA 对应一个定时器服务实例。 */
    private transient Map<NFA<IN>, InternalTimerService<VoidNamespace>> timerServices;

    /** 当前管理的模式处理器及其对应的 NFA 映射。 */
    private transient Map<PatternProcessor<IN>, NFA<IN>> patternProcessors;

    /** 模式处理器集合的序列化器，用于管理其序列化和反序列化。 */
    private transient SimpleVersionedSerializer<List<PatternProcessor<IN>>> patternProcessorSerializer;

    /** 提供给用户函数的上下文。 */
    private transient ContextFunctionImpl context;

    /** 输出收集器，用于设置流记录的时间戳。 */
    private transient TimestampedCollector<OUT> collector;

    /** 限制基础运行时上下文功能的封装上下文。 */
    private transient CepRuntimeContext cepRuntimeContext;

    /** 为 NFA 提供时间相关特性的轻量级上下文。 */
    private transient Map<Tuple2<String, Integer>, TimerService> cepTimerServices;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    /** 存储模式匹配次数的度量指标映射。 */
    private transient Map<Tuple2<String, Integer>, Counter> patternMatchedTimesMetricMap;

    /** 存储延迟事件丢弃次数的度量指标映射。 */
    private transient Map<Tuple2<String, Integer>, Counter> numLateRecordsDroppedMetricMap;

    /** 存储模式匹配平均时间的度量指标映射。 */
    private transient Map<Tuple2<String, Integer>, CepOperator.SimpleGauge<Long>> patternMatchingAvgTimeMetricMap;

    /** 存储模式匹配次数的映射。 */
    private transient Map<Tuple2<String, Integer>, Long> patternMatchingTimesMap;

    /** 存储模式匹配总时间的映射。 */
    private transient Map<Tuple2<String, Integer>, Long> patternMatchingTotalTimeMap;


    /**
     * 构造一个 DynamicCepOperator 实例。
     *
     * @param inputSerializer 输入事件的序列化器。
     * @param isProcessingTime 是否使用处理时间语义。
     */
    public DynamicCepOperator(
            final TypeSerializer<IN> inputSerializer, final boolean isProcessingTime) {
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer, "输入序列化器不能为空");
        this.isProcessingTime = isProcessingTime;
    }


    private static String getNameSuffixedWithPatternProcessor(
            String name, Tuple2<String, Integer> patternProcessorIdentifier) {
        return String.format(
                "%s-%s-%s", name, patternProcessorIdentifier.f0, patternProcessorIdentifier.f1);
    }

    /**
     * 设置操作符的运行时环境。
     *
     * <p>此方法初始化运行时上下文，并为操作符配置必要的上下文和输出。
     *
     * @param containingTask 包含该操作符的任务。
     * @param config 操作符的配置。
     * @param output 操作符的输出。
     */
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
    }


    /**
     * 初始化操作符的状态。
     *
     * @param context 状态初始化上下文，提供状态存储和恢复的支持。
     * @throws Exception 如果初始化失败，则抛出异常。
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        initializationContext = context;
    }


    /**
     * 打开操作符。
     *
     * <p>此方法在任务开始运行时调用，用于初始化运行时的上下文和度量指标。
     *
     * @throws Exception 如果初始化失败，则抛出异常。
     */
    @Override
    public void open() throws Exception {
        super.open();
        context = new ContextFunctionImpl();
        collector = new TimestampedCollector<>(output);
        numLateRecordsDroppedMetricMap = new HashMap<>();
        patternMatchedTimesMetricMap = new HashMap<>();
        patternMatchingAvgTimeMetricMap = new HashMap<>();
        patternMatchingTimesMap = new HashMap<>();
        patternMatchingTotalTimeMap = new HashMap<>();
        cepTimerServices = new HashMap<>();
    }


    /**
     * 关闭操作符。
     *
     * <p>此方法在任务停止运行时调用，用于释放资源并关闭相关的模式处理器和状态。
     *
     * @throws Exception 如果关闭过程中发生错误，则抛出异常。
     */
    @Override
    public void close() throws Exception {
        super.close();

        if (patternProcessors != null) {
            for (Map.Entry<PatternProcessor<IN>, NFA<IN>> entry : patternProcessors.entrySet()) {
                FunctionUtils.closeFunction(entry.getKey().getPatternProcessFunction());
                entry.getValue().close();
            }
        }

        if (partialMatches != null) {
            for (Map.Entry<NFA<IN>, SharedBuffer<IN>> entry : partialMatches.entrySet()) {
                entry.getValue().releaseCacheStatisticsTimer();
            }
        }
    }


    /**
     * 处理单个输入流记录。
     *
     * <p>此方法根据时间语义（处理时间或事件时间）对事件进行不同的处理逻辑：
     * - 在处理时间下，直接处理事件，因为不存在乱序问题。
     * - 在事件时间下，缓冲乱序事件并等待水印触发。
     *
     * @param element 要处理的流记录，包含事件数据和时间戳。
     * @throws Exception 如果事件处理失败，则抛出异常。
     */
    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (patternProcessors != null) {
            // 遍历所有模式处理器及其对应的 NFA（非确定性有限状态机）
            for (Map.Entry<PatternProcessor<IN>, NFA<IN>> entry : patternProcessors.entrySet()) {
                final NFA<IN> nfa = entry.getValue();
                if (isProcessingTime) {
                    // 获取匹配后跳过策略，默认为 noSkip
                    final AfterMatchSkipStrategy afterMatchSkipStrategy =
                            Optional.ofNullable(
                                            entry.getKey()
                                                    .getPattern(this.getUserCodeClassloader())
                                                    .getAfterMatchSkipStrategy())
                                    .orElse(AfterMatchSkipStrategy.noSkip());

                    // 获取当前模式处理器和对应的处理函数
                    final PatternProcessor<IN> patternProcessor = entry.getKey();
                    final PatternProcessFunction<IN, ?> patternProcessFunction =
                            patternProcessor.getPatternProcessFunction();

                    // 获取当前 NFA 的状态
                    final NFAState nfaState = getNFAState(nfa);

                    // 获取当前处理时间
                    long timestamp = getProcessingTimeService().getCurrentProcessingTime();

                    // 推进时间，并处理当前事件
                    advanceTime(patternProcessor, nfa, nfaState, timestamp, patternProcessFunction);
                    processEvent(
                            nfa,
                            nfaState,
                            element.getValue(),
                            timestamp,
                            afterMatchSkipStrategy,
                            patternProcessor);

                    // 更新 NFA 的状态
                    updateNFA(nfa, nfaState);
                } else {
                    // 获取事件时间戳和事件值
                    long timestamp = element.getTimestamp();
                    IN value = element.getValue();

                    // 在事件时间处理模式下，假设水印的正确性：
                    // 1. 如果事件时间小于或等于上一个水印时间，则视为延迟事件。
                    // 2. 如果事件时间大于水印时间，则缓冲事件并等待水印触发。

                    if (timestamp > timerServices.get(nfa).currentWatermark()) {
                        // 如果事件时间有效，则缓冲事件，等待适当的水印触发处理
                        saveRegisterWatermarkTimer(nfa);
                        bufferEvent(nfa, value, timestamp);
                    } else {
                        // 如果事件时间小于或等于水印，则将事件视为延迟事件，记录相关度量
                        numLateRecordsDroppedMetricMap
                                .get(Tuple2.of(entry.getKey().getId(), entry.getKey().getVersion()))
                                .inc();
                    }
                }
            }
        } else {
            // 如果没有可用的模式处理器，记录日志信息
            LOG.info(
                    "There are no available pattern processors in the {}",
                    this.getClass().getSimpleName());
        }
    }

    /**
     * 为指定的 NFA 注册一个事件时间定时器，触发时间为当前水印 + 1。
     *
     * <p>此方法确保每当水印推进时，定时器会触发，从而处理缓冲的乱序事件队列。
     * 注册的定时器以 {@link VoidNamespace} 为命名空间，表示全局性。
     *
     * @param nfa 对应的非确定性有限状态机（NFA）。
     */
    private void saveRegisterWatermarkTimer(NFA<IN> nfa) {
        // 获取当前水印
        long currentWatermark = timerServices.get(nfa).currentWatermark();

        // 防止溢出：只有在当前水印 + 1 不会溢出的情况下才注册定时器
        if (currentWatermark + 1 > currentWatermark) {
            timerServices
                    .get(nfa) // 获取与 NFA 关联的定时器服务
                    .registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1); // 注册定时器
        }
    }


    /**
     * 缓冲乱序事件到指定的时间戳队列中。
     *
     * <p>在事件时间语义中，当事件的时间戳大于当前水印时，事件会被视为乱序事件，
     * 缓存到对应时间戳的队列中，等待后续水印推进时进行处理。
     *
     * @param nfa 对应的非确定性有限状态机（NFA）。
     * @param event 要缓冲的事件。
     * @param currentTime 事件的时间戳。
     * @throws Exception 如果更新缓冲队列失败，则抛出异常。
     */
    private void bufferEvent(NFA<IN> nfa, IN event, long currentTime) throws Exception {
        // 获取当前时间戳的事件队列
        List<IN> elementsForTimestamp = elementQueueStates.get(nfa).get(currentTime);

        // 如果队列为空，则初始化
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>();
        }

        // 将事件添加到队列中
        elementsForTimestamp.add(event);

        // 更新队列到状态存储中
        elementQueueStates.get(nfa).put(currentTime, elementsForTimestamp);
    }


    /**
     * 在事件时间触发时回调。
     *
     * <p>此方法用于处理缓冲的乱序事件，并推进 NFA 的时间到当前水印。
     * 它的主要步骤包括：
     * 1. 获取待处理事件的队列和对应的 NFA。
     * 2. 按事件时间顺序处理缓冲的事件。
     * 3. 将 NFA 的时间推进到当前水印，清理过期的模式。
     * 4. 更新存储的状态，仅保留后续需要使用的 NFA 和 MapState。
     * 5. 更新最后一次看到的水印，并注册下一个定时器。
     *
     * @param timer 提供触发上下文的定时器。
     * @throws Exception 如果在事件处理过程中发生错误，则抛出异常。
     */
    @Override
    public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in event time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) advance the time to the current watermark, so that expired patterns are discarded.
        // 4) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.
        // 5) update the last seen watermark.

        if (patternProcessors != null) {
            // 遍历所有模式处理器及其对应的 NFA（非确定性有限状态机）
            for (Map.Entry<PatternProcessor<IN>, NFA<IN>> entry : patternProcessors.entrySet()) {
                // 获取匹配后跳过策略，默认为 noSkip
                final AfterMatchSkipStrategy afterMatchSkipStrategy =
                        Optional.ofNullable(
                                        entry.getKey()
                                                .getPattern(this.getUserCodeClassloader())
                                                .getAfterMatchSkipStrategy())
                                .orElse(AfterMatchSkipStrategy.noSkip());

                // 获取当前模式处理器和对应的处理函数
                final PatternProcessor<IN> patternProcessor = entry.getKey();
                final PatternProcessFunction<IN, ?> patternProcessFunction =
                        patternProcessor.getPatternProcessFunction();

                // 获取当前模式对应的 NFA
                final NFA<IN> nfa = entry.getValue();


                // STEP 1
                // 获取缓冲事件的时间戳，并按顺序排序
                final PriorityQueue<Long> sortedTimestamps = getSortedTimestamps(nfa);

                // 获取当前 NFA 的状态
                final NFAState nfaState = getNFAState(nfa);


                // STEP 2
                while (!sortedTimestamps.isEmpty()
                        && sortedTimestamps.peek() <= timerServices.get(nfa).currentWatermark()) {
                    // 获取并移除队列中最小的时间戳
                    long timestamp = sortedTimestamps.poll();

                    // 推进时间到当前时间戳
                    advanceTime(patternProcessor, nfa, nfaState, timestamp, patternProcessFunction);

                    // 按时间戳处理对应的缓冲事件
                    try (Stream<IN> elements = elementQueueStates.get(nfa).get(timestamp).stream()) {
                        elements.forEachOrdered(
                                event -> {
                                    try {
                                        processEvent(
                                                nfa,
                                                nfaState,
                                                event,
                                                timestamp,
                                                afterMatchSkipStrategy,
                                                patternProcessor);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                    }

                    // 移除已处理的时间戳的事件队列
                    elementQueueStates.get(nfa).remove(timestamp);
                }

                // STEP 3
                // 推进 NFA 的时间到当前水印
                advanceTime(
                        patternProcessor,
                        nfa,
                        nfaState,
                        timerServices.get(nfa).currentWatermark(),
                        patternProcessFunction);

                // STEP 4
                // 更新 NFA 的状态，仅保留需要的状态
                updateNFA(nfa, nfaState);

                if (!sortedTimestamps.isEmpty() || !partialMatches.get(nfa).isEmpty()) {
                    saveRegisterWatermarkTimer(nfa);
                }
            }
        }
    }

    /**
     * 在处理时间触发时回调。
     *
     * <p>此方法用于按处理时间顺序处理缓冲事件，并更新对应的 NFA 状态。
     * 与事件时间语义不同，处理时间语义不依赖水印，直接按时间顺序处理事件。
     *
     * <p>主要步骤：
     * 1. 获取待处理事件的队列和对应的 NFA。
     * 2. 按处理时间顺序处理缓冲的事件。
     * 3. 更新存储的 NFA 和 MapState 状态，仅保留后续需要使用的状态。
     *
     * @param timer 定时器上下文，提供触发时间的信息。
     * @throws Exception 如果在事件处理过程中发生错误，则抛出异常。
     */
    @Override
    public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
        // 1) get the queue of pending elements for the key and the corresponding NFA,
        // 2) process the pending elements in process time order and custom comparator if exists
        //		by feeding them in the NFA
        // 3) update the stored state for the key, by only storing the new NFA and MapState iff they
        //		have state to be used later.

        if (patternProcessors != null) {
            // 遍历所有模式处理器及其对应的 NFA（非确定性有限状态机）
            for (Map.Entry<PatternProcessor<IN>, NFA<IN>> entry : patternProcessors.entrySet()) {
                // 获取匹配后跳过策略，默认为 noSkip
                final AfterMatchSkipStrategy afterMatchSkipStrategy =
                        Optional.ofNullable(
                                        entry.getKey()
                                                .getPattern(this.getUserCodeClassloader())
                                                .getAfterMatchSkipStrategy())
                                .orElse(AfterMatchSkipStrategy.noSkip());

                // 获取当前模式处理器和对应的处理函数
                final PatternProcessor<IN> patternProcessor = entry.getKey();
                final PatternProcessFunction<IN, ?> patternProcessFunction =
                        patternProcessor.getPatternProcessFunction();

                // 获取当前模式对应的 NFA
                final NFA<IN> nfa = entry.getValue();


                // STEP 1
                // 获取缓冲事件的时间戳，并按顺序排序
                final PriorityQueue<Long> sortedTimestamps = getSortedTimestamps(nfa);

                // 获取当前 NFA 的状态
                final NFAState nfaState = getNFAState(nfa);

                // STEP 2
                while (!sortedTimestamps.isEmpty()) {
                    // 获取并移除队列中最小的时间戳
                    long timestamp = sortedTimestamps.poll();

                    // 推进时间到当前时间戳
                    advanceTime(patternProcessor, nfa, nfaState, timestamp, patternProcessFunction);

                    // 按时间戳处理对应的缓冲事件
                    try (Stream<IN> elements = elementQueueStates.get(nfa).get(timestamp).stream()) {
                        elements.forEachOrdered(
                                event -> {
                                    try {
                                        processEvent(
                                                nfa,
                                                nfaState,
                                                event,
                                                timestamp,
                                                afterMatchSkipStrategy,
                                                patternProcessor);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                    }

                    // 移除已处理的时间戳的事件队列
                    elementQueueStates.get(nfa).remove(timestamp);
                }

                // STEP 3
                // 更新 NFA 的状态，仅保留需要的状态
                updateNFA(nfa, nfaState);
            }
        }
    }

    /**
     * 处理操作符事件。
     *
     * <p>此方法会根据事件类型调用相应的处理逻辑：
     * - 如果事件为 {@link UpdatePatternProcessorEvent}，则更新模式处理器。
     * - 如果事件类型不匹配，则抛出异常。
     *
     * @param event 操作符事件。
     * @throws FlinkRuntimeException 如果事件处理失败或更新模式处理器过程中出现错误。
     */
    @Override
    @SuppressWarnings("unchecked")
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof UpdatePatternProcessorEvent) {
            try {
                // 处理模式处理器更新事件
                handleUpdatePatternProcessorEvent((UpdatePatternProcessorEvent<IN>) event);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Failed to deserialize the pattern processors.", e);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to create the NFA from the pattern processors.", e);
            }
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
    }

    /**
     * 获取指定 NFA 的状态。
     *
     * <p>此方法从托管状态存储中获取 NFA 的当前状态。如果状态不存在（如初次运行或状态丢失），
     * 则创建一个新的初始状态。
     *
     * @param nfa 非确定性有限状态机（NFA）。
     * @return 当前 NFA 的状态。如果状态不存在，则返回初始状态。
     * @throws IOException 如果状态访问或创建过程中发生错误。
     */
    private NFAState getNFAState(NFA<IN> nfa) throws IOException {
        // 从托管状态中获取 NFA 的当前状态
        NFAState nfaState = computationStates.get(nfa).value();

        // 如果状态为空，则创建初始状态
        return nfaState != null ? nfaState : nfa.createInitialNFAState();
    }


    /**
     * 更新 NFA 的状态。
     *
     * <p>此方法在状态发生更改时，将其保存到托管状态存储中。
     * 更新完成后，重置状态的更改标志。
     *
     * @param nfa 非确定性有限状态机（NFA）。
     * @param nfaState NFA 的当前状态。
     * @throws IOException 如果状态更新过程中发生错误。
     */
    private void updateNFA(NFA<IN> nfa, NFAState nfaState) throws IOException {
        // 检查状态是否已更改
        if (nfaState.isStateChanged()) {
            // 重置状态的更改标志
            nfaState.resetStateChanged();

            // 更新状态到托管存储中
            computationStates.get(nfa).update(nfaState);
        }
    }


    /**
     * 获取与指定 NFA 相关联的事件时间戳，并按升序排序。
     *
     * <p>此方法从托管状态中获取所有缓冲事件的时间戳，并返回排序后的优先队列。
     *
     * @param nfa 非确定性有限状态机（NFA）。
     * @return 包含排序后时间戳的优先队列。
     * @throws Exception 如果状态访问过程中发生错误。
     */
    private PriorityQueue<Long> getSortedTimestamps(NFA<IN> nfa) throws Exception {
        // 创建一个优先队列用于存储时间戳
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();

        // 从缓冲状态中获取所有时间戳并加入优先队列
        for (Long timestamp : elementQueueStates.get(nfa).keys()) {
            sortedTimestamps.offer(timestamp);
        }

        // 返回排序后的时间戳队列
        return sortedTimestamps;
    }


    /**
     * 将指定事件传递给 NFA（非确定性有限状态机）进行处理，并输出匹配的事件序列。
     *
     * <p>此方法负责以下主要操作：
     * 1. 使用共享缓冲区（`SharedBuffer`）访问器传递事件。
     * 2. 调用 NFA 的 `process` 方法进行状态更新和匹配计算。
     * 3. 记录匹配处理时间，并更新相关度量指标。
     * 4. 处理并输出匹配的事件序列。
     *
     * @param nfa 处理事件的 NFA（非确定性有限状态机）。
     * @param nfaState NFA 的当前状态。
     * @param event 当前待处理的事件。
     * @param timestamp 当前事件的时间戳。
     * @param afterMatchSkipStrategy 匹配后跳过策略，用于确定如何处理后续事件。
     * @param patternProcessor 当前的模式处理器。
     * @throws Exception 如果处理过程中发生错误，则抛出异常。
     */
    private void processEvent(
            NFA<IN> nfa,
            NFAState nfaState,
            IN event,
            long timestamp,
            AfterMatchSkipStrategy afterMatchSkipStrategy,
            PatternProcessor<IN> patternProcessor)
            throws Exception {
        // 使用 NFA 的共享缓冲区访问器处理事件
        try (SharedBufferAccessor<IN> sharedBufferAccessor =
                partialMatches.get(nfa).getAccessor()) {
            // 获取模式处理器的标识符（包括 ID 和版本）
            Tuple2<String, Integer> patternProcessorIdentifier =
                    Tuple2.of(patternProcessor.getId(), patternProcessor.getVersion());
            // 获取对应的定时器服务，用于时间相关操作
            TimerService timerService = cepTimerServices.get(patternProcessorIdentifier);
            // 记录事件处理开始时间
            long processStartTime = System.nanoTime();
            // 将事件传递给 NFA 进行处理，并返回匹配的事件序列
            Collection<Map<String, List<IN>>> patterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event,
                            timestamp,
                            afterMatchSkipStrategy,
                            timerService);
            // 记录事件处理结束时间
            long processEndTime = System.nanoTime();

            // 更新总处理时间指标
            patternMatchingTotalTimeMap.merge(
                    patternProcessorIdentifier, processEndTime - processStartTime, Long::sum);

            // 更新处理次数指标
            patternMatchingTimesMap.merge(patternProcessorIdentifier, 1L, Long::sum);

            // 更新匹配平均时间指标
            patternMatchingAvgTimeMetricMap
                    .get(patternProcessorIdentifier)
                    .update(
                            patternMatchingTotalTimeMap.get(patternProcessorIdentifier)
                                    / patternMatchingTimesMap.get(patternProcessorIdentifier));
            // 处理匹配的事件序列并输出结果
            processMatchedSequences(patternProcessor, patterns, timestamp);
        }
    }

    /**
     * 推进给定 NFA 的时间到指定时间戳。
     *
     * <p>此方法确保在推进时间后，NFA 不会再处理时间戳小于指定时间戳的事件。
     * 这可能会导致以下情况：
     * 1. 剪枝：清理不再可能匹配的状态。
     * 2. 超时：触发超时逻辑，处理超时的部分匹配结果。
     *
     * @param patternProcessor 当前的模式处理器，用于处理匹配的事件序列。
     * @param nfa 非确定性有限状态机（NFA），用于进行模式匹配。
     * @param nfaState 当前 NFA 的状态。
     * @param timestamp 要推进到的时间戳。
     * @param function 模式处理函数，用于处理匹配结果和超时的部分匹配结果。
     * @throws Exception 如果在推进时间或处理匹配结果过程中发生错误。
     */
    private void advanceTime(
            PatternProcessor<IN> patternProcessor,
            NFA<IN> nfa,
            NFAState nfaState,
            long timestamp,
            PatternProcessFunction<IN, ?> function)
            throws Exception {
        // 使用 NFA 的共享缓冲区访问器，用于存取状态和匹配数据
        try (SharedBufferAccessor<IN> sharedBufferAccessor =
                partialMatches.get(nfa).getAccessor()) {

            // 调用 NFA 的时间推进方法，获取匹配结果和超时的部分匹配结果
            Tuple2<
                            Collection<Map<String, List<IN>>>,
                            Collection<Tuple2<Map<String, List<IN>>, Long>>>
                    pendingMatchesAndTimeout =
                            nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);

            // 获取匹配的事件序列
            Collection<Map<String, List<IN>>> pendingMatches = pendingMatchesAndTimeout.f0;
            // 获取超时的部分匹配结果
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut = pendingMatchesAndTimeout.f1;
            // 如果存在匹配的事件序列，进行处理
            if (!pendingMatches.isEmpty()) {
                processMatchedSequences(patternProcessor, pendingMatches, timestamp);
            }
            // 如果存在超时的部分匹配结果，进行处理
            if (!timedOut.isEmpty()) {
                processTimedOutSequences(function, timedOut);
            }
        }
    }

    /**
     * 处理并输出匹配的事件序列。
     *
     * <p>此方法对匹配的事件序列逐一进行处理，并将其传递给模式处理函数，
     * 以生成最终的输出结果。同时更新模式匹配的度量指标。
     *
     * @param patternProcessor 当前的模式处理器。
     * @param matchingSequences 匹配的事件序列集合，每个序列是一个键值对映射，键为模式名称，值为事件列表。
     * @param timestamp 当前匹配的时间戳。
     * @throws Exception 如果处理匹配序列时发生错误。
     */
    private void processMatchedSequences(
            PatternProcessor<IN> patternProcessor,
            Iterable<Map<String, List<IN>>> matchingSequences,
            long timestamp)
            throws Exception {
        // 设置当前时间戳，用于后续输出记录或上下文信息
        setTimestamp(timestamp);
        // 遍历所有匹配的事件序列
        for (Map<String, List<IN>> matchingSequence : matchingSequences) {
            // 根据模式处理器的 ID 和版本获取对应的度量指标，并增加匹配次数
            patternMatchedTimesMetricMap
                    .get(Tuple2.of(patternProcessor.getId(), patternProcessor.getVersion()))
                    .inc();
            // 将当前模式处理器设置到上下文中，供后续处理使用
            context.setPatternProcessor(patternProcessor);
            ((PatternProcessFunction<IN, OUT>) patternProcessor.getPatternProcessFunction())
                    .processMatch(matchingSequence, context, collector);
        }
    }

    /**
     * 处理超时的部分匹配结果。
     *
     * <p>此方法针对模式匹配中超时的部分匹配序列调用用户定义的超时处理函数。
     * 仅当模式处理函数实现了 {@link TimedOutPartialMatchHandler} 接口时，才会进行处理。
     *
     * @param function 模式处理函数，可能包含超时处理逻辑。
     * @param timedOutSequences 超时的部分匹配结果集合，每个超时结果包含：
     *                          - 一个部分匹配的事件序列（键值对映射）。
     *                          - 对应的超时时间戳。
     * @throws Exception 如果处理超时匹配结果时发生错误。
     */
    private void processTimedOutSequences(
            PatternProcessFunction<IN, ?> function,
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences)
            throws Exception {
        // 检查模式处理函数是否实现了 TimedOutPartialMatchHandler 接口
        if (function instanceof TimedOutPartialMatchHandler) {

            // 强制转换为 TimedOutPartialMatchHandler 类型
            @SuppressWarnings("unchecked")
            TimedOutPartialMatchHandler<IN> timeoutHandler =
                    (TimedOutPartialMatchHandler<IN>) function;

            // 遍历所有超时的部分匹配结果
            for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
                // 设置当前时间戳到上下文中
                setTimestamp(matchingSequence.f1);

                // 调用用户定义的超时处理逻辑
                timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
            }
        }
    }


    /**
     * 设置当前时间戳。
     *
     * <p>此方法会将时间戳设置到输出收集器和上下文中。
     * 仅当时间语义为事件时间时，会将绝对时间戳设置到输出收集器中。
     *
     * @param timestamp 当前时间戳。
     */
    private void setTimestamp(long timestamp) {
        // 如果时间语义为事件时间，则设置绝对时间戳到收集器
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }

        // 设置时间戳到上下文
        context.setTimestamp(timestamp);
    }

    /**
     * 处理更新模式处理器的事件。
     *
     * <p>此方法从事件中获取模式处理器列表，并重新初始化相关状态、计时器和度量指标。
     * 具体包括：
     * 1. 初始化新的模式处理器及其对应的 NFA（非确定性有限状态机）。
     * 2. 为每个模式处理器创建共享缓冲区、状态、计时器和度量指标。
     * 3. 如果状态已恢复，将旧状态迁移到新初始化的结构中。
     *
     * @param event 包含新模式处理器的事件。
     * @throws Exception 如果处理过程中发生错误。
     */
    private void handleUpdatePatternProcessorEvent(UpdatePatternProcessorEvent<IN> event)
            throws Exception {
        // 从事件中提取模式处理器列表
        final List<PatternProcessor<IN>> eventPatternProcessors = event.patternProcessors();

        // 初始化模式处理器及其相关映射
        patternProcessors = new HashMap<>(eventPatternProcessors.size());
        computationStates = new HashMap<>(eventPatternProcessors.size());
        elementQueueStates = new HashMap<>(eventPatternProcessors.size());
        partialMatches = new HashMap<>(eventPatternProcessors.size());
        timerServices = new HashMap<>(eventPatternProcessors.size());

        // 初始化计时器服务和度量指标的映射
        final Map<Tuple2<String, Integer>, TimerService> newCepTimerServices =
                new HashMap<>(eventPatternProcessors.size());

        // Metrics
        final Map<Tuple2<String, Integer>, Counter> newNumLateRecordsDroppedMetricMap =
                new HashMap<>(eventPatternProcessors.size());
        final Map<Tuple2<String, Integer>, Counter> newPatternMatchedTimesMetricMap =
                new HashMap<>(eventPatternProcessors.size());
        final Map<Tuple2<String, Integer>, CepOperator.SimpleGauge<Long>>
                newPatternMatchingAvgTimeMap = new HashMap<>(eventPatternProcessors.size());

        for (PatternProcessor<IN> patternProcessor : eventPatternProcessors) {
            // 获取模式处理器的标识符（ID 和版本）
            final Tuple2<String, Integer> patternProcessorIdentifier =
                    Tuple2.of(patternProcessor.getId(), patternProcessor.getVersion());
            // 获取模式处理函数并初始化其运行时上下文
            final PatternProcessFunction<IN, ?> patternProcessFunction =
                    patternProcessor.getPatternProcessFunction();
            FunctionUtils.setFunctionRuntimeContext(patternProcessFunction, cepRuntimeContext);
            FunctionUtils.openFunction(patternProcessFunction, new Configuration());

            // 编译模式处理器的 NFA
            final NFACompiler.NFAFactory<IN> nfaFactory =
                    NFACompiler.compileFactory(
                            patternProcessor.getPattern(this.getUserCodeClassloader()),
                            patternProcessFunction instanceof TimedOutPartialMatchHandler);

            // 创建并初始化 NFA
            final NFA<IN> nfa = nfaFactory.createNFA();
            nfa.open(cepRuntimeContext, new Configuration());

            // 创建共享缓冲区
            final SharedBuffer<IN> partialMatch =
                    new SharedBuffer<>(
                            initializationContext.getKeyedStateStore(),
                            inputSerializer,
                            SharedBufferCacheConfig.of(getOperatorConfig().getConfiguration()),
                            patternProcessor,
                            eventPatternProcessors.size());
            // initializeState through the provided context
            // 创建并初始化 NFA 状态
            final ValueState<NFAState> computationState =
                    initializationContext
                            .getKeyedStateStore()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            getNameSuffixedWithPatternProcessor(
                                                    NFA_STATE_NAME, patternProcessorIdentifier),
                                            new NFAStateSerializer()));

            // 创建并初始化事件队列状态
            final MapState<Long, List<IN>> elementQueueState =
                    initializationContext
                            .getKeyedStateStore()
                            .getMapState(
                                    new MapStateDescriptor<>(
                                            getNameSuffixedWithPatternProcessor(
                                                    EVENT_QUEUE_STATE_NAME,
                                                    patternProcessorIdentifier),
                                            LongSerializer.INSTANCE,
                                            new ListSerializer<>(inputSerializer)));

            // 如果已恢复状态，则迁移旧状态到新结构
            if (initializationContext.isRestored()) {
                partialMatch.migrateOldState(getKeyedStateBackend(), computationState);
            }

            // 初始化内部计时器服务
            final InternalTimerService<VoidNamespace> timerService =
                    getInternalTimerService(
                            String.format(
                                    "watermark-callbacks-%s-%s",
                                    patternProcessor.getId(), patternProcessor.getVersion()),
                            VoidNamespaceSerializer.INSTANCE,
                            this);
            // 初始化 CEP 的计时器服务
            final TimerService cepTimerService = new TimerServiceImpl(nfa);
            // 将模式处理器及其相关组件存入映射
            patternProcessors.put(patternProcessor, nfa);

            LOG.info(
                    "Use patternProcessors version: "
                            + patternProcessor.getVersion()
                            + " with "
                            + patternProcessor.getPattern(this.getUserCodeClassloader()));
            computationStates.put(nfa, computationState);
            elementQueueStates.put(nfa, elementQueueState);
            partialMatches.put(nfa, partialMatch);
            timerServices.put(nfa, timerService);
            cepTimerServices.put(patternProcessorIdentifier, cepTimerService);
            newCepTimerServices.put(
                    patternProcessorIdentifier,
                    cepTimerServices.getOrDefault(patternProcessorIdentifier, cepTimerService));
            // metrics
            // 更新延迟事件丢弃次数的度量指标
            this.addCounterMetric(
                    LATE_ELEMENTS_DROPPED_METRIC_NAME,
                    numLateRecordsDroppedMetricMap,
                    newNumLateRecordsDroppedMetricMap,
                    patternProcessorIdentifier);
            // 更新模式匹配次数的度量指标
            this.addCounterMetric(
                    PATTERN_MATCHED_TIMES_METRIC_NAME,
                    patternMatchedTimesMetricMap,
                    newPatternMatchedTimesMetricMap,
                    patternProcessorIdentifier);
            // 更新模式匹配平均时间的度量指标
            this.addLongGaugeMetric(
                    PATTERN_MATCHING_AVG_TIME_METRIC_NAME,
                    patternMatchingAvgTimeMetricMap,
                    newPatternMatchingAvgTimeMap,
                    patternProcessorIdentifier);
        }
        // 更新为新的度量指标和计时器服务映射
        numLateRecordsDroppedMetricMap = newNumLateRecordsDroppedMetricMap;
        patternMatchedTimesMetricMap = newPatternMatchedTimesMetricMap;
        patternMatchingAvgTimeMetricMap = newPatternMatchingAvgTimeMap;
        cepTimerServices = newCepTimerServices;
    }

    /**
     * 获取当前模式处理器的数量。
     *
     * <p>此方法主要用于测试，返回当前管理的模式处理器的数量。
     *
     * @return 当前模式处理器的数量。
     */
    @VisibleForTesting
    int getNumOfPatternProcessors() {
        return patternProcessors.size();
    }


    /**
     * 获取指定模式处理器的匹配次数。
     *
     * <p>此方法主要用于测试，返回指定模式处理器的匹配事件次数。
     *
     * @param identifier 模式处理器的标识符，由 ID 和版本号组成。
     * @return 匹配事件的次数。
     */
    @VisibleForTesting
    long getPatternMatchedTimes(Tuple2<String, Integer> identifier) {
        return patternMatchedTimesMetricMap.get(identifier).getCount();
    }


    /**
     * 获取指定模式处理器的延迟事件数量。
     *
     * <p>此方法主要用于测试，返回指定模式处理器因延迟而丢弃的事件数量。
     *
     * @param identifier 模式处理器的标识符，由 ID 和版本号组成。
     * @return 延迟事件的数量。
     */
    @VisibleForTesting
    long getLateRecordsNumber(Tuple2<String, Integer> identifier) {
        return numLateRecordsDroppedMetricMap.get(identifier).getCount();
    }


    /**
     * 获取指定模式处理器的平均匹配时间。
     *
     * <p>此方法主要用于测试，返回指定模式处理器的事件匹配平均耗时。
     *
     * @param identifier 模式处理器的标识符，由 ID 和版本号组成。
     * @return 平均匹配时间。
     */
    @VisibleForTesting
    long getPatternMatchingAvgTime(Tuple2<String, Integer> identifier) {
        return patternMatchingAvgTimeMetricMap.get(identifier).getValue();
    }


    /**
     * 添加计数器度量指标。
     *
     * <p>此方法为指定的模式处理器添加计数器度量指标，用于统计事件匹配或延迟丢弃的数量。
     *
     * @param metricName 度量指标的名称。
     * @param oldMetricMap 现有的度量指标映射，用于判断是否已有指标。
     * @param newMetricMap 新的度量指标映射，用于存储新添加的指标。
     * @param identifier 模式处理器的标识符，由 ID 和版本号组成。
     */
    private void addCounterMetric(
            String metricName,
            Map<Tuple2<String, Integer>, Counter> oldMetricMap,
            Map<Tuple2<String, Integer>, Counter> newMetricMap,
            Tuple2<String, Integer> identifier) {
        // 如果旧映射中不存在指定指标，则创建新计数器，否则复用旧指标
        final Counter patternMatchedTimes =
                !oldMetricMap.containsKey(identifier)
                        ? metrics.counter(
                        getNameSuffixedWithPatternProcessor(metricName, identifier))
                        : oldMetricMap.get(identifier);
        newMetricMap.put(identifier, patternMatchedTimes);
    }


    /**
     * 添加长整型仪表度量指标。
     *
     * <p>此方法为指定的模式处理器添加仪表度量指标，用于监控事件匹配的平均耗时。
     *
     * @param metricName 度量指标的名称。
     * @param oldMetricMap 现有的度量指标映射，用于判断是否已有指标。
     * @param newMetricMap 新的度量指标映射，用于存储新添加的指标。
     * @param identifier 模式处理器的标识符，由 ID 和版本号组成。
     */
    private void addLongGaugeMetric(
            String metricName,
            Map<Tuple2<String, Integer>, CepOperator.SimpleGauge<Long>> oldMetricMap,
            Map<Tuple2<String, Integer>, CepOperator.SimpleGauge<Long>> newMetricMap,
            Tuple2<String, Integer> identifier) {
        // 如果旧映射中不存在指定指标，则创建新仪表，否则复用旧指标
        final CepOperator.SimpleGauge<Long> patternMatchedTimes =
                !oldMetricMap.containsKey(identifier)
                        ? metrics.gauge(
                        getNameSuffixedWithPatternProcessor(metricName, identifier),
                        new CepOperator.SimpleGauge<>(0L))
                        : oldMetricMap.get(identifier);
        newMetricMap.put(identifier, patternMatchedTimes);
    }

    /**
     * 为 {@link NFA} 提供访问 {@link InternalTimerService} 的功能，同时判断当前 {@link DynamicCepOperator}
     * 是否基于处理时间运行。
     *
     * <p>此类的每个实例与一个 {@link NFA} 绑定，提供以下功能：
     * - 获取当前处理时间。
     * - 为 NFA 提供定时器服务的支持。
     *
     * <p>此类应在每个操作符实例中创建一次。
     */
    private class TimerServiceImpl implements TimerService {

        /** 与该 TimerService 绑定的 NFA 实例。 */
        private final NFA<?> nfa;

        /**
         * 构造函数，初始化与指定 NFA 关联的 TimerService。
         *
         * @param nfa 非确定性有限状态机（NFA）。
         */
        public TimerServiceImpl(NFA<?> nfa) {
            this.nfa = nfa;
        }

        /**
         * 获取当前处理时间。
         *
         * @return 当前处理时间（以毫秒为单位）。
         */
        @Override
        public long currentProcessingTime() {
            // 从关联的定时器服务中获取当前处理时间
            return timerServices.get(nfa).currentProcessingTime();
        }
    }


    /**
     * {@link PatternProcessFunction.Context} 的实现类。
     *
     * <p>此类在每个操作符中创建一次，提供以下功能：
     * - 通过 {@link InternalTimerService} 获取当前处理时间。
     * - 获取当前记录的时间戳（事件时间语义）或 null（处理时间语义）。
     * - 支持基于事件时间或处理时间的侧输出。
     *
     * <p>此类主要为用户定义的模式处理函数提供上下文支持。
     */
    public class ContextFunctionImpl implements PatternProcessFunction.Context {

        /** 当前记录的时间戳（事件时间语义） */
        private Long timestamp;

        /** 当前关联的模式处理器 */
        private PatternProcessor<?> patternProcessor;

        /**
         * 将结果输出到指定的侧输出流。
         *
         * <p>根据时间语义（事件时间或处理时间），为输出记录设置适当的时间戳。
         *
         * @param outputTag 输出的侧输出标记。
         * @param value 要输出的值。
         * @param <X> 输出值的类型。
         */
        @Override
        public <X> void output(final OutputTag<X> outputTag, final X value) {
            final StreamRecord<X> record;
            // 根据时间语义设置记录的时间戳
            if (isProcessingTime) {
                record = new StreamRecord<>(value); // 处理时间语义下不设置时间戳
            } else {
                record = new StreamRecord<>(value, timestamp()); // 事件时间语义下设置时间戳
            }
            // 将记录输出到指定的侧输出流
            output.collect(outputTag, record);
        }

        /**
         * 设置当前关联的模式处理器。
         *
         * @param patternProcessor 当前的模式处理器。
         */
        void setPatternProcessor(PatternProcessor<?> patternProcessor) {
            this.patternProcessor = patternProcessor;
        }

        /**
         * 设置当前记录的时间戳。
         *
         * @param timestamp 当前记录的时间戳。
         */
        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * 获取当前关联的模式处理器。
         *
         * @return 当前的模式处理器。
         */
        public PatternProcessor<?> patternProcessor() {
            return patternProcessor;
        }

        /**
         * 获取当前记录的时间戳。
         *
         * @return 当前记录的时间戳。
         */
        @Override
        public long timestamp() {
            return timestamp;
        }

        /**
         * 获取当前处理时间。
         *
         * @return 当前处理时间（以毫秒为单位）。
         */
        @Override
        public long currentProcessingTime() {
            // 从定时器服务中获取当前处理时间
            return timerServices
                    .get(patternProcessors.get(patternProcessor))
                    .currentProcessingTime();
        }
    }

}
