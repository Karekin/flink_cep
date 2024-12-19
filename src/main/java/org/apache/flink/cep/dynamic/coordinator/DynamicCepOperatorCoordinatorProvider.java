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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadFactory;

/**
 * DynamicCepOperatorCoordinator 的提供者类。
 *
 * <p>该类负责创建和管理 {@link DynamicCepOperatorCoordinator} 的实例。它是 Flink 的
 * {@link RecreateOnResetOperatorCoordinator.Provider} 的实现，支持在操作符重置时重新创建协调器。
 *
 * @param <T> 模式中出现的元素的基础类型。
 */

public class DynamicCepOperatorCoordinatorProvider<T>
        extends RecreateOnResetOperatorCoordinator.Provider {

    /** 操作符的名称。 */
    private final String operatorName;

    /** 模式处理器发现器的工厂，用于动态发现模式处理器。 */
    private final PatternProcessorDiscovererFactory<T> discovererFactory;

    /** 用于序列化的版本号，确保类的兼容性。 */
    private static final long serialVersionUID = 1L;

    /**
     * 构造一个 DynamicCepOperatorCoordinatorProvider 实例。
     *
     * @param operatorName 操作符的名称。
     * @param operatorID 该协调器对应的操作符 ID。
     * @param discovererFactory 模式处理器发现器的工厂，用于动态发现新的模式处理器。
     */
    public DynamicCepOperatorCoordinatorProvider(
            String operatorName,
            OperatorID operatorID,
            PatternProcessorDiscovererFactory<T> discovererFactory) {
        super(operatorID); // 调用父类构造函数
        this.operatorName = operatorName;
        this.discovererFactory = discovererFactory;
    }


    /**
     * 创建并返回一个新的 {@link DynamicCepOperatorCoordinator} 实例。
     *
     * @param context 操作符协调器上下文，提供运行时的环境和资源。
     * @return 新创建的 {@link DynamicCepOperatorCoordinator} 实例。
     */
    @Override
    public OperatorCoordinator getCoordinator(OperatorCoordinator.Context context) {
        // 协调器线程名称
        final String coordinatorThreadName = "PatternProcessorCoordinator-" + operatorName;

        // 创建协调器线程工厂
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        coordinatorThreadName, context.getUserCodeClassloader());

        // 创建协调器上下文
        DynamicCepOperatorCoordinatorContext coordinatorContext =
                new DynamicCepOperatorCoordinatorContext(coordinatorThreadFactory, context);

        // 返回新的 DynamicCepOperatorCoordinator 实例
        return new DynamicCepOperatorCoordinator<>(
                operatorName, discovererFactory, coordinatorContext);
    }


    /**
     * 一个线程工厂类，用于为协调器创建专用线程。
     *
     * <p>该类还提供了一些辅助方法，例如设置线程的异常处理程序，以及判断当前线程是否为协调器线程。
     */

    public static class CoordinatorExecutorThreadFactory
            implements ThreadFactory, Thread.UncaughtExceptionHandler {

        /** 协调器线程的名称。 */
        private final String coordinatorThreadName;

        /** 协调器线程的类加载器，用于加载上下文相关的类。 */
        private final ClassLoader classLoader;

        /** 线程的未捕获异常处理程序。 */
        private final Thread.UncaughtExceptionHandler errorHandler;

        /** 当前协调器线程的引用。 */
        @Nullable private Thread thread;


        // TODO discuss if we should fail the job(JM may restart the job later) or directly kill JM
        /**
         * 使用默认的异常处理程序创建线程工厂。
         *
         * @param coordinatorThreadName 协调器线程的名称。
         * @param contextClassLoader 线程的上下文类加载器。
         */
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final ClassLoader contextClassLoader) {
            this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
        }


        /**
         * 使用指定的异常处理程序创建线程工厂。
         *
         * @param coordinatorThreadName 协调器线程的名称。
         * @param contextClassLoader 线程的上下文类加载器。
         * @param errorHandler 线程的未捕获异常处理程序。
         */
        @VisibleForTesting
        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.classLoader = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        /**
         * 创建一个新的协调器线程。
         *
         * @param r 要在新线程中执行的任务。
         * @return 新创建的线程。
         */
        @Override
        public synchronized Thread newThread(Runnable r) {
            thread = new Thread(r, coordinatorThreadName);
            thread.setContextClassLoader(classLoader); // 设置线程的上下文类加载器
            thread.setUncaughtExceptionHandler(this); // 设置未捕获异常处理程序
            return thread;
        }


        /**
         * 处理线程中的未捕获异常。
         *
         * @param t 发生异常的线程。
         * @param e 未捕获的异常。
         */
        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorHandler.uncaughtException(t, e); // 调用异常处理程序
        }


        /**
         * 获取协调器线程的名称。
         *
         * @return 协调器线程的名称。
         */
        public String getCoordinatorThreadName() {
            return coordinatorThreadName;
        }


        /**
         * 判断当前线程是否为协调器线程。
         *
         * @return 如果当前线程是协调器线程，则返回 true；否则返回 false。
         */
        boolean isCurrentThreadCoordinatorThread() {
            return Thread.currentThread() == thread;
        }

    }
}
