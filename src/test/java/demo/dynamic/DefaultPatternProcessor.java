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

package demo.dynamic;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * DefaultPatternProcessor 是 {@link PatternProcessor} 的默认实现类，
 * 提供了一个可配置的模式处理器。
 *
 * <p>此类支持：
 * - 使用字符串形式的模式定义（`patternStr`）。
 * - 动态加载模式处理函数 {@link PatternProcessFunction}。
 *
 * @param <T> 表示出现在模式中的元素类型，同时也是基于匹配结果生成的元素类型。
 */

@PublicEvolving
public class DefaultPatternProcessor<T> implements PatternProcessor<T> {

    /** 模式处理器的唯一标识符，用于标识此处理器实例。 */
    private final String id;

    /** 模式处理器的版本号，用于管理和区分不同版本的处理器。 */
    private final Integer version;

    /** 模式的字符串表示形式，用于定义模式逻辑。 */
    private final String patternStr;

    /**
     * 模式处理函数 {@link PatternProcessFunction}，用于处理匹配到的结果。
     *
     * <p>此字段可以为 null，表示无需额外处理匹配结果。
     */
    private final @Nullable PatternProcessFunction<T, ?> patternProcessFunction;


    /**
     * 构造一个 DefaultPatternProcessor 实例。
     *
     * @param id 模式处理器的唯一标识符。
     * @param version 模式处理器的版本号。
     * @param pattern 模式的字符串表示形式。
     * @param patternProcessFunction 模式处理函数，可为 null。
     * @param userCodeClassLoader 用户代码类加载器，用于动态加载模式定义和处理逻辑。
     * @throws NullPointerException 如果 id、version 或 pattern 为 null，则抛出异常。
     */
    public DefaultPatternProcessor(
            final String id,
            final Integer version,
            final String pattern,
            final @Nullable PatternProcessFunction<T, ?> patternProcessFunction,
            final ClassLoader userCodeClassLoader) {
        this.id = checkNotNull(id, "Pattern Processor ID 不能为空");
        this.version = checkNotNull(version, "Pattern Processor 版本号不能为空");
        this.patternStr = checkNotNull(pattern, "Pattern 定义不能为空");
        this.patternProcessFunction = patternProcessFunction;
    }


    @Override
    public String toString() {
        return "DefaultPatternProcessor{"
                + "id='"
                + id
                + '\''
                + ", version="
                + version
                + ", pattern="
                + patternStr
                + ", patternProcessFunction="
                + patternProcessFunction
                + '}';
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public int getVersion() {
        return version;
    }

    /**
     * 使用指定的类加载器加载并解析模式。
     *
     * <p>此方法将模式的字符串形式（`patternStr`）转换为 {@link Pattern} 对象。
     *
     * @param classLoader 类加载器，用于动态加载模式定义。
     * @return 解析后的 {@link Pattern} 对象。
     * @throws RuntimeException 如果解析模式字符串失败，则抛出异常。
     */
    @Override
    public Pattern<T, ?> getPattern(ClassLoader classLoader) {
        try {
            return (Pattern<T, ?>) CepJsonUtils.convertJSONStringToPattern(patternStr, classLoader);
        } catch (Exception e) {
            throw new RuntimeException("模式解析失败: " + e.getMessage(), e);
        }
    }


    /**
     * 获取用于处理匹配结果的模式处理函数。
     *
     * <p>如果未显式提供模式处理函数，此方法将返回一个默认的 {@link DemoPatternProcessFunction}。
     *
     * @return 模式处理函数 {@link PatternProcessFunction}。
     */
    @Override
    public PatternProcessFunction<T, ?> getPatternProcessFunction() {
        return patternProcessFunction != null ? patternProcessFunction : new DemoPatternProcessFunction<>();
    }

}
