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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.cep.dynamic.coordinator.DynamicCepOperatorCoordinatorProvider.CoordinatorExecutorThreadFactory;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link OperatorCoordinator} 的上下文类。
 *
 * <p>该类负责为协调器提供线程模型和上下文支持，主要功能包括：
 * - 确保所有协调器状态操作均由同一线程处理。
 * - 提供对 Flink 操作符协调器上下文的访问。
 * - 管理子任务网关，并负责事件的发送与接收。
 *
 * @see OperatorCoordinator
 */

@Internal
public class DynamicCepOperatorCoordinatorContext implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicCepOperatorCoordinatorContext.class);

    /** 用于协调器的单线程执行器，确保协调器状态的线程安全操作。 */
    private final ScheduledExecutorService coordinatorExecutor;

    /** 用于后台任务的单线程执行器，例如数据处理或异步操作。 */
    private final ScheduledExecutorService workerExecutor;

    /** 协调器线程工厂，用于创建协调器专用线程。 */
    private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;

    /** Flink 的操作符协调器上下文，用于访问运行时相关信息。 */
    private final OperatorCoordinator.Context operatorCoordinatorContext;

    /** 子任务网关的映射表，用于与每个子任务通信。 */
    private final Map<Integer, OperatorCoordinator.SubtaskGateway> subtaskGateways;

    /** 时钟实例，用于时间操作。 */
    private static final Clock clock = SystemClock.getInstance();


    /**
     * 使用指定的线程工厂和 Flink 操作符协调器上下文创建上下文。
     *
     * @param coordinatorThreadFactory 协调器线程工厂，用于创建协调器线程。
     * @param operatorCoordinatorContext Flink 操作符协调器上下文。
     */
    public DynamicCepOperatorCoordinatorContext(
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this(
                Executors.newScheduledThreadPool(1, coordinatorThreadFactory),
                Executors.newScheduledThreadPool(
                        1,
                        new ExecutorThreadFactory(
                                coordinatorThreadFactory.getCoordinatorThreadName() + "-worker")),
                coordinatorThreadFactory,
                operatorCoordinatorContext);
    }


    /**
     * 使用自定义的执行器和线程工厂创建上下文。
     *
     * @param coordinatorExecutor 协调器的执行器。
     * @param workerExecutor 后台任务的执行器。
     * @param coordinatorThreadFactory 协调器线程工厂。
     * @param operatorCoordinatorContext Flink 操作符协调器上下文。
     */
    public DynamicCepOperatorCoordinatorContext(
            ScheduledExecutorService coordinatorExecutor,
            ScheduledExecutorService workerExecutor,
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this.coordinatorExecutor = coordinatorExecutor;
        this.workerExecutor = workerExecutor;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.subtaskGateways = new HashMap<>(operatorCoordinatorContext.currentParallelism());
    }


    /**
     * 关闭协调器上下文。
     *
     * <p>此方法强制关闭所有执行器，以确保线程安全地释放资源。
     *
     * @throws InterruptedException 如果关闭过程中被中断，则抛出异常。
     */
    @Override
    public void close() throws InterruptedException {
        // Close quietly so the closing sequence will be executed completely.
        shutdownExecutorForcefully(workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        shutdownExecutorForcefully(coordinatorExecutor, Duration.ofNanos(Long.MAX_VALUE));
    }


    /**
     * 在协调器线程中执行指定的操作。
     *
     * @param runnable 要执行的操作。
     */
    public void runInCoordinatorThread(Runnable runnable) {
        coordinatorExecutor.execute(runnable);
    }

    // --------- Package private additional methods for the PatternProcessorCoordinator ------------
    /**
     * 获取用户代码的类加载器。
     *
     * @return 用户代码的类加载器。
     */
    ClassLoader getUserCodeClassloader() {
        return this.operatorCoordinatorContext.getUserCodeClassloader();
    }


    /**
     * 标记子任务已准备好，并为其设置网关。
     *
     * @param gateway 子任务的网关。
     * @throws IllegalStateException 如果指定子任务已存在网关，则抛出异常。
     */
    void subtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        final int subtask = gateway.getSubtask();
        if (subtaskGateways.get(subtask) == null) {
            subtaskGateways.put(subtask, gateway);
        } else {
            throw new IllegalStateException("Already have a subtask gateway for " + subtask);
        }
    }


    /**
     * 标记子任务未准备好。
     *
     * @param subtaskIndex 子任务索引。
     */
    void subtaskNotReady(int subtaskIndex) {
        subtaskGateways.put(subtaskIndex, null);
    }


    /**
     * 获取所有子任务的索引。
     *
     * @return 子任务索引的集合。
     */
    Set<Integer> getSubtasks() {
        return subtaskGateways.keySet();
    }


    /**
     * 向指定子任务发送事件。
     *
     * @param subtaskId 子任务的 ID。
     * @param event 要发送的事件。
     */
    public void sendEventToOperator(int subtaskId, OperatorEvent event) {
        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway = subtaskGateways.get(subtaskId);
                    if (gateway == null) {
                        LOG.warn(
                                String.format(
                                        "Subtask %d is not ready yet to receive events.",
                                        subtaskId));
                    } else {
                        gateway.sendEvent(event);
                    }
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    /**
     * Fail the job with the given cause.
     *
     * @param cause the cause of the job failure.
     */
    void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause);
    }

    // ---------------- private helper methods -----------------

    /**
     * 在协调器线程中执行指定的操作。
     *
     * <p>此方法确保指定的操作由协调器线程执行。如果当前线程不是协调器线程，或者协调器执行器未关闭，
     * 则操作会被提交到协调器执行器中。否则直接在当前线程中执行操作。
     *
     * <p>如果操作执行过程中发生异常，将捕获并记录日志，同时抛出 {@link FlinkRuntimeException}。
     *
     * @param callable 要执行的操作，返回值类型为 V。
     * @param errorMessage 如果操作失败时的错误消息，用于抛出异常时的描述。
     * @param <V> 返回值的类型。
     * @return 操作的返回结果。
     * @throws FlinkRuntimeException 如果操作执行失败或抛出未捕获的异常。
     */
    private <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // 确保分配操作在协调器线程中执行。
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread() // 检查当前线程是否为协调器线程
                && !coordinatorExecutor.isShutdown()) { // 检查协调器执行器是否已关闭
            try {
                // 包装要执行的操作，确保异常被捕获并重新抛出
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                // 执行操作
                                return callable.call();
                            } catch (Throwable t) {
                                // 捕获未处理的异常并记录日志
                                LOG.error("Uncaught Exception in Source Coordinator Executor", t);
                                // 重新抛出异常
                                ExceptionUtils.rethrowException(t);
                                return null; // 此代码实际上不可达，但需要以满足 Callable 接口
                            }
                        };

                // 提交操作到协调器线程执行，并等待结果返回
                return coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                // 捕获中断或执行异常，并封装为 FlinkRuntimeException 抛出
                throw new FlinkRuntimeException(errorMessage, e);
            }
        }

        try {
            // 如果当前线程是协调器线程，则直接执行操作
            return callable.call();
        } catch (Throwable t) {
            // 捕获未处理的异常并记录日志
            LOG.error("Uncaught Exception in Source Coordinator Executor", t);
            // 抛出封装的 FlinkRuntimeException
            throw new FlinkRuntimeException(errorMessage, t);
        }

    }

    /**
     * Shutdown the given executor forcefully within the given timeout.
     *
     * @param executor the executor to shut down.
     * @param timeout the timeout duration.
     */
    /**
     * 强制关闭指定的执行器。
     *
     * @param executor 要关闭的执行器。
     * @param timeout 关闭的超时时间。
     */
    private void shutdownExecutorForcefully(ExecutorService executor, Duration timeout) {
        Deadline deadline = Deadline.fromNowWithClock(timeout, clock);
        boolean isInterrupted = false;
        do {
            executor.shutdownNow();
            try {
                executor.awaitTermination(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        } while (!isInterrupted && deadline.hasTimeLeft() && !executor.isTerminated());
    }
}
