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

package org.apache.flink.cep.dynamic.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cep.dynamic.operator.DynamicCepOperator;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.cep.dynamic.processor.PatternProcessorManager;
import org.apache.flink.cep.dynamic.serializer.PatternProcessorSerializer;
import org.apache.flink.cep.event.UpdatePatternProcessorEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.cep.dynamic.coordinator.DynamicCepOperatorCoordinatorSerdeUtils.readBytes;
import static org.apache.flink.cep.dynamic.coordinator.DynamicCepOperatorCoordinatorSerdeUtils.verifyCoordinatorSerdeVersion;
import static org.apache.flink.cep.dynamic.coordinator.DynamicCepOperatorCoordinatorSerdeUtils.writeCoordinatorSerdeVersion;
import static org.apache.flink.util.IOUtils.closeAll;

/**
 * DynamicCepOperatorCoordinator 是 {@link OperatorCoordinator} 的默认实现，
 * 专为 {@link DynamicCepOperator} 提供协调功能。
 *
 * <p>此类采用事件循环风格的线程模型，与 Flink 运行时交互，并确保所有状态操作都在事件循环线程中执行。
 *
 * <p>主要功能包括：
 * - 管理模式处理器的发现和更新。
 * - 维护协调器的状态，包括模式处理器列表和序列化器。
 * - 支持 Flink 的检查点功能，以保证状态一致性。
 * - 处理子任务的失败、重置和恢复。
 *
 * @param <T> 出现在模式中的元素的基础类型。
 */

@Internal
public class DynamicCepOperatorCoordinator<T>
        implements OperatorCoordinator, PatternProcessorManager<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicCepOperatorCoordinator.class);

    /** 操作符的名称，与该协调器关联。 */
    private final String operatorName;

    /** 模式处理器发现器工厂，用于创建模式处理器发现器实例。 */
    private final PatternProcessorDiscovererFactory<T> discovererFactory;

    /** 协调器的上下文，包含其管理的状态和运行时环境。 */
    private final DynamicCepOperatorCoordinatorContext context;

    /** 用于模式处理器集合的序列化和反序列化的序列化器。 */
    private PatternProcessorSerializer<T> patternProcessorSerializer;

    /** 模式处理器发现器，用于处理模式处理器的更新。 */
    private PatternProcessorDiscoverer<T> discoverer;

    /** 当前管理的模式处理器列表。 */
    private List<PatternProcessor<T>> currentPatternProcessors = new ArrayList<>();

    /** 标记协调器是否已启动。 */
    private boolean started;


    /**
     * 构造一个 DynamicCepOperatorCoordinator 实例。
     *
     * @param operatorName 操作符的名称。
     * @param discovererFactory 模式处理器发现器工厂。
     * @param context 协调器上下文，包含运行时的状态和操作。
     */
    public DynamicCepOperatorCoordinator(
            String operatorName,
            PatternProcessorDiscovererFactory<T> discovererFactory,
            DynamicCepOperatorCoordinatorContext context) {
        this.operatorName = operatorName;
        this.discovererFactory = discovererFactory;
        this.context = context;
    }


    /**
     * 启动协调器。
     *
     * <p>此方法负责初始化模式处理器发现器，并启动模式处理器更新的检测任务。
     *
     * @throws Exception 如果启动失败，则抛出异常。
     */
    @Override
    public void start() throws Exception {
        LOG.info(
                "Starting pattern processor discoverer for {}: {}.",
                this.getClass().getSimpleName(),
                operatorName);

        // we mark this as started first, so that we can later distinguish the cases where 'start()'
        // wasn't called and where 'start()' failed.
        started = true;

        // 初始化序列化器
        patternProcessorSerializer = new PatternProcessorSerializer<>();

        // 如果发现器尚未创建，则通过工厂创建
        if (discoverer == null) {
            try {
                discoverer =
                        discovererFactory.createPatternProcessorDiscoverer(
                                context.getUserCodeClassloader());
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to create PatternProcessorDiscoverer for pattern processor {}",
                        operatorName,
                        t);
                context.failJob(t);
                return;
            }
        }

        // 启动模式处理器更新的检测任务
        // The pattern processor discovery is the first task in the coordinator executor.
        // We rely on the single-threaded coordinator executor to guarantee
        // the other methods are invoked after the discoverer has discovered.
        runInEventLoop(
                () -> discoverer.discoverPatternProcessorUpdates(this),
                "discovering the PatternProcessor updates.");
    }


    /**
     * 关闭协调器。
     *
     * <p>释放协调器中持有的资源，包括模式处理器发现器和上下文。
     *
     * @throws Exception 如果关闭过程中出现异常，则抛出。
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing PatternProcessorCoordinator for pattern processor {}.", operatorName);
        if (started) {
            closeAll(context, discoverer);
        }
        started = false;
        LOG.info("PatternProcessor coordinator for pattern processor {} closed.", operatorName);
    }


    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        // no op
    }

    /**
     * 为协调器创建检查点（checkpoint）。
     *
     * <p>此方法序列化协调器的状态，并将其提交到 Flink 的检查点存储。
     *
     * @param checkpointId 检查点的 ID。
     * @param resultFuture 检查点结果的异步任务。
     * @throws Exception 如果创建检查点失败，则抛出异常。
     */
    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        runInEventLoop(
                () -> {
                    LOG.debug(
                            "Taking a state snapshot on operator {} for checkpoint {}",
                            operatorName,
                            checkpointId);
                    try {
                        resultFuture.complete(toBytes());
                    } catch (Throwable e) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
                        resultFuture.completeExceptionally(
                                new CompletionException(
                                        String.format(
                                                "Failed to checkpoint the current PatternProcessor for pattern processor %s",
                                                operatorName),
                                        e));
                    }
                },
                "taking checkpoint %d",
                checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as completed for pattern processor {}.",
                checkpointId,
                operatorName);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOG.info(
                "Marking checkpoint {} as aborted for pattern processor {}.",
                checkpointId,
                operatorName);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {

        // The checkpoint data is null if there was no completed checkpoint before in that case we
        // don't restore here, but let a fresh PatternProcessorDiscoverer be created when "start()"
        // is called.
        if (checkpointData == null) {
            return;
        }

        LOG.info(
                "Restoring PatternProcessorDiscoverer of pattern processor {} from checkpoint.",
                operatorName);
        currentPatternProcessors = deserializeCheckpoint(checkpointData);
        discoverer =
                discovererFactory.createPatternProcessorDiscoverer(
                        context.getUserCodeClassloader());
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        runInEventLoop(
                () -> {
                    LOG.info(
                            "Removing itself after failure for subtask {} of pattern processor {}.",
                            subtask,
                            operatorName);
                    context.subtaskNotReady(subtask);
                },
                "handling subtask %d failure",
                subtask);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        LOG.info(
                "Recovering subtask {} to checkpoint {} for pattern processor {} to checkpoint.",
                subtask,
                checkpointId,
                operatorName);
        runInEventLoop(
                () -> {
                    context.sendEventToOperator(
                            subtask, new UpdatePatternProcessorEvent<>(currentPatternProcessors));
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        assert subtask == gateway.getSubtask();
        LOG.debug("Subtask {} of pattern processor {} is ready.", subtask, operatorName);
        runInEventLoop(
                () -> {
                    context.subtaskReady(gateway);
                    context.sendEventToOperator(
                            subtask, new UpdatePatternProcessorEvent<>(currentPatternProcessors));
                },
                "making event gateway to subtask %d available",
                subtask);
    }

    /**
     * 当模式处理器更新时调用。
     *
     * <p>此方法更新当前的模式处理器列表，并将更新发送到所有子任务。
     *
     * @param patternProcessors 更新后的模式处理器列表。
     */
    @Override
    public void onPatternProcessorsUpdated(List<PatternProcessor<T>> patternProcessors) {
        try {
            checkPatternProcessors(patternProcessors);
        } catch (Exception e) {
            LOG.error(
                    "Failed to send UpdatePatternProcessorEvent to pattern processor due to check failure {}",
                    operatorName,
                    e);
            context.failJob(e);
            return;
        }

        // 向所有子任务发送更新事件
        // Sends the UpdatePatternProcessorEvent to PatternProcessorOperator with the updated
        // pattern processors.
        for (int subtask : context.getSubtasks()) {
            try {
                context.sendEventToOperator(
                        subtask, new UpdatePatternProcessorEvent<>(patternProcessors));
            } catch (IOException e) {
                LOG.error(
                        "Failed to send UpdatePatternProcessorEvent to pattern processor {}",
                        operatorName,
                        e);
                context.failJob(e);
                return;
            }
        }

        currentPatternProcessors = patternProcessors;
        LOG.info("PatternProcessors have been updated.");
        currentPatternProcessors.forEach(
                patternProcessor -> LOG.debug("new PatternProcessors: " + patternProcessor));
    }

    /**
     * 获取模式处理器发现器工厂。
     *
     * <p>此方法标注为 {@link VisibleForTesting}，仅供测试使用，返回协调器的模式处理器发现器工厂实例。
     *
     * @return 当前的 {@link PatternProcessorDiscovererFactory} 实例。
     */
    @VisibleForTesting
    PatternProcessorDiscovererFactory<T> getPatternProcessorDiscovererFactory() {
        return this.discovererFactory;
    }


    /**
     * 获取模式处理器发现器。
     *
     * <p>此方法标注为 {@link VisibleForTesting}，仅供测试使用，返回协调器的模式处理器发现器实例。
     *
     * @return 当前的 {@link PatternProcessorDiscoverer} 实例。
     */
    @VisibleForTesting
    PatternProcessorDiscoverer<T> getDiscoverer() {
        return discoverer;
    }


    /**
     * 获取当前的模式处理器列表。
     *
     * <p>此方法标注为 {@link VisibleForTesting}，仅供测试使用，返回协调器当前管理的模式处理器列表。
     *
     * @return 当前的模式处理器列表。
     */
    @VisibleForTesting
    List<PatternProcessor<T>> getCurrentPatternProcessors() {
        return this.currentPatternProcessors;
    }


    /**
     * 获取协调器的上下文。
     *
     * <p>此方法标注为 {@link VisibleForTesting}，仅供测试使用，返回协调器的上下文对象。
     *
     * @return 当前的 {@link DynamicCepOperatorCoordinatorContext} 实例。
     */
    @VisibleForTesting
    DynamicCepOperatorCoordinatorContext getContext() {
        return context;
    }


    /**
     * 在事件循环中运行指定的操作。
     *
     * <p>此方法将指定的操作（action）提交到协调器的事件循环线程中执行，确保所有状态操作的线程安全。
     *
     * @param action 要在事件循环中执行的操作。
     * @param actionName 操作的描述名称，用于日志记录。
     * @param actionNameFormatParameters 操作名称中的格式化参数，用于动态构建日志消息。
     * @throws IllegalStateException 如果协调器尚未启动，则抛出异常。
     */
    private void runInEventLoop(
            final ThrowingRunnable<Throwable> action,
            final String actionName,
            final Object... actionNameFormatParameters) {

        // 确保协调器已启动
        ensureStarted();

        // 如果发现器未初始化且因子任务失败导致方法调用，则直接返回
        if (discoverer == null) {
            return;
        }

        // 提交操作到协调器的事件循环线程
        context.runInCoordinatorThread(
                () -> {
                    try {
                        // 执行指定操作
                        action.run();
                    } catch (Throwable t) {
                        // 如果遇到 JVM 的致命错误或 OOM，立即抛出以避免后续操作失败
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                        // 构建操作描述字符串
                        final String actionString = String.format(actionName, actionNameFormatParameters);
                        LOG.error(
                                "Uncaught exception in the PatternProcessorDiscover for PatternProcessor {} while {}. Triggering job failover.",
                                operatorName,
                                actionString,
                                t);

                        // 触发作业失败
                        context.failJob(t);
                    }
                });
    }


    // --------------------- Serde -----------------------

    /**
     * 序列化协调器的状态。
     *
     * <p>此方法将协调器的当前状态序列化为字节数组，便于在 Flink 的检查点机制中存储。
     * 当前实现可能不是最高效的，但状态通常较小，因此性能影响较低。
     * 如果状态较大，本身可能会对性能造成影响，而不仅仅是序列化的实现问题。
     *
     * @return 包含序列化状态的字节数组。
     * @throws Exception 如果序列化过程中出现问题，则抛出异常。
     */
    private byte[] toBytes() throws Exception {
        return writeCheckpointBytes();
    }


    /**
     * 将协调器的状态写入字节数组。
     *
     * <p>此方法将协调器的版本、模式处理器的版本以及序列化的模式处理器写入输出流。
     *
     * @return 包含协调器状态的字节数组。
     * @throws Exception 如果序列化过程中出现问题，则抛出异常。
     */
    private byte[] writeCheckpointBytes() throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {

            // 写入协调器的序列化版本
            DynamicCepOperatorCoordinatorSerdeUtils.writeCoordinatorSerdeVersion(out);

            // 写入模式处理器的序列化版本
            out.writeInt(patternProcessorSerializer.getVersion());

            // 序列化模式处理器集合并写入长度和内容
            byte[] serializedCheckpoint =
                    patternProcessorSerializer.serialize(currentPatternProcessors);
            out.writeInt(serializedCheckpoint.length);
            out.write(serializedCheckpoint);

            // 刷新输出流并返回字节数组
            out.flush();
            return baos.toByteArray();
        }
    }


    /**
     * 从字节数组中恢复协调器的状态。
     *
     * <p>此方法将协调器的状态从检查点字节数组中反序列化，恢复模式处理器的状态。
     *
     * @param bytes 从 {@link #toBytes()} 返回的检查点字节数组。
     * @return 恢复的模式处理器列表。
     * @throws Exception 如果反序列化失败，则抛出异常。
     */
    private List<PatternProcessor<T>> deserializeCheckpoint(byte[] bytes) throws Exception {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream in = new DataInputViewStreamWrapper(bais)) {

            // 验证协调器的序列化版本
            DynamicCepOperatorCoordinatorSerdeUtils.verifyCoordinatorSerdeVersion(in);

            // 读取模式处理器的序列化版本
            int serializerVersion = in.readInt();

            // 读取模式处理器的序列化数据并反序列化
            int serializedCheckpointSize = in.readInt();
            byte[] serializedCheckpoint = DynamicCepOperatorCoordinatorSerdeUtils.readBytes(in, serializedCheckpointSize);

            return patternProcessorSerializer.deserialize(serializerVersion, serializedCheckpoint);
        }
    }


    /**
     * 检查模式处理器及其处理函数的有效性。
     *
     * <p>此方法验证模式处理器列表是否为空，并确保每个模式处理器的处理函数符合要求。
     * 具体而言，处理函数不能同时实现 {@link CheckpointedFunction} 和 {@link ListCheckpointed} 接口。
     *
     * @param patternProcessors 要检查的模式处理器列表。
     * @throws NullPointerException 如果模式处理器或其处理函数为空，则抛出异常。
     * @throws IllegalStateException 如果处理函数同时实现了不兼容的接口，则抛出异常。
     */
    private void checkPatternProcessors(List<PatternProcessor<T>> patternProcessors) {
        Preconditions.checkNotNull(patternProcessors, "Patten processor cannot be null.");
        for (PatternProcessor<T> patternProcessor : patternProcessors) {
            Preconditions.checkNotNull(patternProcessor, "Patten processor cannot be null.");
            PatternProcessFunction<T, ?> patternProcessFunction =
                    patternProcessor.getPatternProcessFunction();
            if (patternProcessFunction == null) {
                throw new NullPointerException(
                        String.format(
                                "Patten process function of the pattern processor {id={%s}, version={%d}} is null",
                                patternProcessor.getId(), patternProcessor.getVersion()));
            }

            // 检查处理函数的接口实现
            if (patternProcessFunction instanceof CheckpointedFunction
                    && patternProcessFunction instanceof ListCheckpointed) {
                throw new IllegalStateException(
                        String.format(
                                "Patten process function of the pattern processor {id={%s}, version={%d}} is not allowed to implement CheckpointedFunction AND ListCheckpointed.",
                                patternProcessor.getId(), patternProcessor.getVersion()));
            }
        }
    }


    // --------------------- private methods -------------

    /**
     * 确保协调器已启动。
     *
     * @throws IllegalStateException 如果协调器未启动，则抛出异常。
     */
    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("The coordinator has not started yet.");
        }
    }
}
