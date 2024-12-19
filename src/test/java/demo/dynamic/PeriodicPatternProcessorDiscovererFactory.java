package demo.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscovererFactory;

import javax.annotation.Nullable;
import java.util.List;

/**
 * PeriodicPatternProcessorDiscovererFactory 是 {@link PatternProcessorDiscovererFactory} 的一个抽象实现，
 * 用于创建 {@link PeriodicPatternProcessorDiscoverer} 实例。
 *
 * <p>此类定义了用于周期性检测模式处理器更新的工厂逻辑。实现类需要提供具体的
 * {@link PeriodicPatternProcessorDiscoverer} 实现。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public abstract class PeriodicPatternProcessorDiscovererFactory<T>
        implements PatternProcessorDiscovererFactory<T> {

    /**
     * 初始模式处理器列表，用于在第一次加载时作为参考。
     *
     * <p>可以为 null，表示没有初始模式处理器。
     */
    @Nullable
    private final List<PatternProcessor<T>> initialPatternProcessors;

    /**
     * 检测模式处理器更新的时间间隔（以毫秒为单位）。
     */
    private final Long intervalMillis;


    /**
     * 构造一个 PeriodicPatternProcessorDiscovererFactory 实例。
     *
     * @param initialPatternProcessors 初始模式处理器列表，可为 null。
     * @param intervalMillis 检测模式处理器更新的时间间隔（以毫秒为单位）。
     */
    public PeriodicPatternProcessorDiscovererFactory(
            @Nullable final List<PatternProcessor<T>> initialPatternProcessors,
            @Nullable final Long intervalMillis) {
        this.initialPatternProcessors = initialPatternProcessors;
        this.intervalMillis = intervalMillis;
    }


    /**
     * 获取初始模式处理器列表。
     *
     * @return 初始模式处理器列表，如果未提供，则返回 null。
     */
    @Nullable
    public List<PatternProcessor<T>> getInitialPatternProcessors() {
        return initialPatternProcessors;
    }


    /**
     * 创建一个 {@link PatternProcessorDiscoverer} 实例。
     *
     * <p>子类需要实现此方法，并提供具体的 {@link PeriodicPatternProcessorDiscoverer} 实现。
     *
     * @param userCodeClassLoader 用户代码类加载器，用于动态加载模式处理器相关的类和逻辑。
     * @return 一个 {@link PatternProcessorDiscoverer} 实例。
     * @throws Exception 如果创建失败，则抛出异常。
     */
    @Override
    public abstract PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;


    /**
     * 获取检测模式处理器更新的时间间隔。
     *
     * @return 时间间隔（以毫秒为单位）。
     */
    public Long getIntervalMillis() {
        return intervalMillis;
    }

}
