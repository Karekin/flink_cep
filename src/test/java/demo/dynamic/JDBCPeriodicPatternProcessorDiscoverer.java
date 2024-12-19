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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * JDBCPeriodicPatternProcessorDiscoverer 是 {@link PeriodicPatternProcessorDiscoverer} 的具体实现，
 * 它通过 JDBC 定期从数据库中发现规则更新，并通知模式处理器管理器。
 *
 * <p>此类负责：
 * - 定期从指定的数据库表中查询模式处理器的最新定义。
 * - 检测模式处理器的变化。
 * - 将更新的模式处理器加载到内存中。
 * - 将最新的模式处理器通知给 {PatternProcessorManager}。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public class JDBCPeriodicPatternProcessorDiscoverer<T>
        extends PeriodicPatternProcessorDiscoverer<T> {

    /**
     * 日志记录器，用于记录模式处理器发现过程中的日志信息。
     */
    private static final Logger LOG =
            LoggerFactory.getLogger(JDBCPeriodicPatternProcessorDiscoverer.class);

    /**
     * 数据库表的名称，用于存储模式处理器定义。
     */
    private final String tableName;

    /**
     * 初始模式处理器列表，用于在第一次加载时作为参考。
     */
    private final List<PatternProcessor<T>> initialPatternProcessors;

    /**
     * 用户代码类加载器，用于动态加载模式处理器的类和函数。
     */
    private final ClassLoader userCodeClassLoader;

    /**
     * JDBC 语句对象，用于执行 SQL 查询。
     */
    private Statement statement;

    /**
     * JDBC 结果集对象，用于存储查询结果。
     */
    private ResultSet resultSet;

    /**
     * 最新模式处理器的定义缓存，用于检测变化。
     */
    private Map<String, Tuple4<String, Integer, String, String>> latestPatternProcessors;


    /**
     * 构造一个 JDBCPeriodicPatternProcessorDiscoverer 实例。
     *
     * @param jdbcUrl 数据库的 JDBC URL。
     * @param jdbcDriver 数据库的 JDBC 驱动类名称。
     * @param tableName 模式处理器定义存储的数据库表名。
     * @param userCodeClassLoader 用户代码类加载器，用于动态加载类和函数。
     * @param initialPatternProcessors 初始模式处理器列表。
     * @param intervalMillis 检查更新的时间间隔（以毫秒为单位）。
     * @throws Exception 如果 JDBC 连接初始化失败，则抛出异常。
     */
    public JDBCPeriodicPatternProcessorDiscoverer(
            final String jdbcUrl,
            final String jdbcDriver,
            final String tableName,
            final ClassLoader userCodeClassLoader,
            @Nullable final List<PatternProcessor<T>> initialPatternProcessors,
            @Nullable final Long intervalMillis)
            throws Exception {
        super(intervalMillis);
        this.tableName = requireNonNull(tableName);
        this.initialPatternProcessors = initialPatternProcessors;
        this.userCodeClassLoader = userCodeClassLoader;

        // 加载 JDBC 驱动
        Class.forName(requireNonNull(jdbcDriver));
        // 初始化 JDBC 语句对象
        this.statement = DriverManager.getConnection(requireNonNull(jdbcUrl)).createStatement();
    }


    /**
     * 检查模式处理器是否发生更新。
     *
     * <p>通过对比数据库中模式处理器的最新定义和缓存的定义，判断是否存在变化。
     *
     * @return 如果模式处理器有更新，则返回 true；否则返回 false。
     */
    @Override
    public boolean arePatternProcessorsUpdated() {
        // 如果尚未初始化最新模式处理器缓存且初始模式处理器不为空，直接返回 true
        if (latestPatternProcessors == null
                && !CollectionUtil.isNullOrEmpty(initialPatternProcessors)) {
            return true;
        }

        // 如果 JDBC 语句对象为空，则返回 false
        if (statement == null) {
            return false;
        }

        try {
            // 查询未被删除的模式处理器定义
            resultSet = statement.executeQuery("SELECT * FROM " + tableName + " WHERE is_deleted=0");
            Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors =
                    new HashMap<>();
            while (resultSet.next()) {
                currentPatternProcessors.put(
                        resultSet.getString("id") + "_" + resultSet.getInt("version"),
                        new Tuple4<>(
                                requireNonNull(resultSet.getString("id")),
                                resultSet.getInt("version"),
                                requireNonNull(resultSet.getString("pattern")),
                                resultSet.getString("function")));
            }
            // 如果最新模式处理器缓存为空或当前模式处理器与缓存不一致，返回 true
            if (latestPatternProcessors == null
                    || isPatternProcessorUpdated(currentPatternProcessors)) {
                latestPatternProcessors = currentPatternProcessors;
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            LOG.warn("Pattern processor discoverer checks rule changes - " + e.getMessage());
        }
        return false;
    }


    /**
     * 获取最新的模式处理器列表。
     *
     * <p>通过从数据库中加载最新的模式定义，并解析为模式处理器对象。
     *
     * @return 最新模式处理器的列表。
     * @throws Exception 如果加载或解析模式处理器失败，则抛出异常。
     */
    @SuppressWarnings("unchecked")
    @Override
    public List<PatternProcessor<T>> getLatestPatternProcessors() throws Exception {
        ObjectMapper objectMapper =
                new ObjectMapper()
                        .registerModule(
                                new SimpleModule()
                                        .addDeserializer(
                                                ConditionSpec.class,
                                                ConditionSpecStdDeserializer.INSTANCE)
                                        .addDeserializer(Time.class, TimeStdDeserializer.INSTANCE)
                                        .addDeserializer(
                                                NodeSpec.class, NodeSpecStdDeserializer.INSTANCE));

        // 从缓存中解析模式处理器列表
        return latestPatternProcessors.values().stream()
                .map(
                        patternProcessor -> {
                            try {
                                String patternStr = patternProcessor.f2;
                                GraphSpec graphSpec =
                                        objectMapper.readValue(patternStr, GraphSpec.class);
                                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                                System.out.println(
                                        objectMapper
                                                .writerWithDefaultPrettyPrinter()
                                                .writeValueAsString(graphSpec));
                                PatternProcessFunction<T, ?> patternProcessFunction = null;
                                if (!StringUtils.isNullOrWhitespaceOnly(patternProcessor.f3)) {
                                    patternProcessFunction =
                                            (PatternProcessFunction<T, ?>)
                                                    this.userCodeClassLoader
                                                            .loadClass(patternProcessor.f3)
                                                            .newInstance();
                                }
                                LOG.warn(
                                        objectMapper
                                                .writerWithDefaultPrettyPrinter()
                                                .writeValueAsString(patternProcessor.f2));
                                return new DefaultPatternProcessor<>(
                                        patternProcessor.f0,
                                        patternProcessor.f1,
                                        patternStr,
                                        patternProcessFunction,
                                        this.userCodeClassLoader);
                            } catch (Exception e) {

                                LOG.error(
                                        "Get the latest pattern processors of the discoverer failure. - "
                                                + e.getMessage());
                                e.printStackTrace();
                            }
                            return null;
                        })
                .collect(Collectors.toList());
    }


    /**
     * 关闭 JDBC 资源。
     *
     * <p>关闭结果集和语句对象，并调用父类的关闭方法释放定时器资源。
     *
     * @throws IOException 如果关闭资源时发生异常。
     */
    @Override
    public void close() throws IOException {
        super.close();
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "ResultSet of the pattern processor discoverer couldn't be closed - "
                            + e.getMessage());
        } finally {
            resultSet = null;
        }
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Statement of the pattern processor discoverer couldn't be closed - "
                            + e.getMessage());
        } finally {
            statement = null;
        }
    }


    /**
     * 判断当前模式处理器与缓存的模式处理器是否不同。
     *
     * @param currentPatternProcessors 当前从数据库加载的模式处理器定义。
     * @return 如果有变化，则返回 true；否则返回 false。
     */
    private boolean isPatternProcessorUpdated(
            Map<String, Tuple4<String, Integer, String, String>> currentPatternProcessors) {
        return latestPatternProcessors.size() != currentPatternProcessors.size()
                || !currentPatternProcessors.equals(latestPatternProcessors);
    }

}
