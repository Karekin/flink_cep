package demo.dynamic;

import org.apache.flink.cep.dynamic.operator.DynamicCepOperator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * DemoPatternProcessFunction 是一个简单的 {@link PatternProcessFunction} 实现，用于处理模式匹配的结果。
 *
 * <p>此类的主要功能是：
 * - 处理由 Flink CEP 检测到的模式匹配结果。
 * - 输出匹配模式的 ID、版本和事件序列的描述信息。
 *
 * @param <IN> 输入事件的类型。
 */

public class DemoPatternProcessFunction<IN> extends PatternProcessFunction<IN, String> {

    /**
     * 处理模式匹配结果。
     *
     * <p>此方法会在模式匹配成功时调用，输出匹配模式的 ID、版本以及事件序列的详细信息。
     *
     * @param match 匹配的事件集合。键是模式定义中的模式名称，值是匹配的事件列表。
     * @param ctx 上下文对象，提供与当前匹配相关的元信息。
     * @param out 用于输出匹配结果的收集器。
     */
    @Override
    public void processMatch(
            final Map<String, List<IN>> match, final Context ctx, final Collector<String> out) {
        // 构建输出字符串
        StringBuilder sb = new StringBuilder();

        // 输出匹配模式的 ID 和版本信息
        sb.append("match for Pattern of (id, version): (")
                .append(((DynamicCepOperator.ContextFunctionImpl) ctx).patternProcessor().getId())
                .append(", ")
                .append(
                        ((DynamicCepOperator.ContextFunctionImpl) ctx)
                                .patternProcessor()
                                .getVersion())
                .append(") is found. The event sequence: ");

        // 输出匹配的事件序列
        for (Map.Entry<String, List<IN>> entry : match.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }

        // 将结果输出到收集器
        out.collect(sb.toString());
    }

}
