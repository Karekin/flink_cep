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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KTD, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorManager;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

/**
 * PeriodicPatternProcessorDiscoverer 是 {@link PatternProcessorDiscoverer} 的一个实现类，
 * 它通过定期检查的方式发现模式处理器的更新。
 *
 * <p>此类使用定时器（{@link Timer}）在指定的时间间隔内周期性地检测模式处理器的变化，
 * 并将更新的模式处理器通知给 {@link PatternProcessorManager}。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public abstract class PeriodicPatternProcessorDiscoverer<T>
        implements PatternProcessorDiscoverer<T> {

    /**
     * 检查模式处理器更新的时间间隔（毫秒）。
     */
    private final Long intervalMillis;

    /**
     * 定时器对象，用于周期性地触发模式处理器更新的检查。
     */
    private final Timer timer;


    /**
     * 创建一个新的 PeriodicPatternProcessorDiscoverer。
     *
     * <p>通过指定的时间间隔初始化定时器，用于定期检查模式处理器的更新。
     *
     * @param intervalMillis 检查更新的时间间隔（以毫秒为单位）。
     */
    public PeriodicPatternProcessorDiscoverer(final Long intervalMillis) {
        this.intervalMillis = intervalMillis;
        this.timer = new Timer();
    }


    /**
     * 检查是否存在更新的模式处理器。
     *
     * <p>子类需要实现此方法，用于定义如何判断模式处理器是否发生变化。
     *
     * @return 如果存在更新的模式处理器，则返回 true；否则返回 false。
     */
    public abstract boolean arePatternProcessorsUpdated();


    /**
     * 获取最新的模式处理器列表。
     *
     * <p>子类需要实现此方法，用于定义如何获取更新的模式处理器。
     *
     * @return 最新的模式处理器列表。
     * @throws Exception 如果获取最新的模式处理器失败，则抛出异常。
     */
    public abstract List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception;


    /**
     * 定期发现模式处理器更新。
     *
     * <p>此方法启动定时器，按照指定的时间间隔周期性地检查模式处理器的更新。
     * 如果检测到模式处理器发生变化，则获取最新的模式处理器，并通知 {@link PatternProcessorManager}。
     *
     * @param patternProcessorManager 模式处理器管理器，用于接收模式处理器更新的通知。
     */
    @Override
    public void discoverPatternProcessorUpdates(
            PatternProcessorManager<T> patternProcessorManager) {
        // 定期调度任务
        timer.schedule(
                new TimerTask() {
                    @Override
                    public void run() {
                        // 检查是否有更新的模式处理器
                        if (arePatternProcessorsUpdated()) {
                            List<PatternProcessor<T>> patternProcessors = null;
                            try {
                                // 获取最新的模式处理器
                                patternProcessors = getLatestPatternProcessors();
                            } catch (Exception e) {
                                e.printStackTrace(); // 打印异常，便于调试
                            }
                            // 通知 PatternProcessorManager 更新模式处理器
                            patternProcessorManager.onPatternProcessorsUpdated(patternProcessors);
                        }
                    }
                },
                0, // 第一次执行的延迟时间
                intervalMillis); // 任务执行的时间间隔
    }


    /**
     * 关闭定时器并释放资源。
     *
     * <p>当不再需要检测模式处理器更新时，应调用此方法以停止定时器任务并释放相关资源。
     *
     * @throws IOException 如果资源释放失败，则抛出异常。
     */
    @Override
    public void close() throws IOException {
        timer.cancel(); // 取消所有已调度的任务
    }

}
