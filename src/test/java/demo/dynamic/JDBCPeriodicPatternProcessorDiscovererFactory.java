package demo.dynamic;

import org.apache.flink.cep.dynamic.processor.PatternProcessor;
import org.apache.flink.cep.dynamic.processor.PatternProcessorDiscoverer;

import javax.annotation.Nullable;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * JDBCPeriodicPatternProcessorDiscovererFactory 是 {@link PeriodicPatternProcessorDiscovererFactory} 的具体实现，
 * 用于创建 {@link JDBCPeriodicPatternProcessorDiscoverer} 实例。
 *
 * <p>此工厂通过 JDBC 从数据库中读取模式处理器的定义，并使用定时机制定期检查模式处理器的更新。
 * 它适用于基于数据库管理模式处理器的动态发现场景。
 *
 * @param <T> 表示出现在模式中的元素的基础类型。
 */

public class JDBCPeriodicPatternProcessorDiscovererFactory<T>
        extends PeriodicPatternProcessorDiscovererFactory<T> {

    /**
     * 数据库的 JDBC URL，用于连接目标数据库。
     */
    private final String jdbcUrl;

    /**
     * 数据库的 JDBC 驱动类名称，用于加载数据库驱动。
     */
    private final String jdbcDriver;

    /**
     * 存储模式处理器定义的数据库表名。
     */
    private final String tableName;


    /**
     * 构造一个 JDBCPeriodicPatternProcessorDiscovererFactory 实例。
     *
     * @param jdbcUrl 数据库的 JDBC URL。
     * @param jdbcDriver 数据库的 JDBC 驱动类名称。
     * @param tableName 存储模式处理器定义的数据库表名。
     * @param initialPatternProcessors 初始模式处理器列表，可为 null。
     * @param intervalMillis 检测模式处理器更新的时间间隔（以毫秒为单位），可为 null。
     * @throws NullPointerException 如果 jdbcUrl、jdbcDriver 或 tableName 为 null，则抛出异常。
     */
    public JDBCPeriodicPatternProcessorDiscovererFactory(
            final String jdbcUrl,
            final String jdbcDriver,
            final String tableName,
            @Nullable final List<PatternProcessor<T>> initialPatternProcessors,
            @Nullable final Long intervalMillis) {
        super(initialPatternProcessors, intervalMillis);
        this.jdbcUrl = requireNonNull(jdbcUrl, "JDBC URL 不能为空");
        this.jdbcDriver = requireNonNull(jdbcDriver, "JDBC 驱动不能为空");
        this.tableName = requireNonNull(tableName, "表名不能为空");
    }


    /**
     * 创建一个 {@link JDBCPeriodicPatternProcessorDiscoverer} 实例。
     *
     * <p>该发现器通过 JDBC 定期从数据库中检查模式处理器的定义，并通知模式处理器管理器进行更新。
     *
     * @param userCodeClassLoader 用户代码类加载器，用于动态加载模式处理器相关的类和逻辑。
     * @return 一个 {@link JDBCPeriodicPatternProcessorDiscoverer} 实例。
     * @throws Exception 如果发现器创建失败，则抛出异常。
     */
    @Override
    public PatternProcessorDiscoverer<T> createPatternProcessorDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception {
        // 创建并返回 JDBCPeriodicPatternProcessorDiscoverer 实例
        return new JDBCPeriodicPatternProcessorDiscoverer<>(
                jdbcUrl,
                jdbcDriver,
                tableName,
                userCodeClassLoader,
                this.getInitialPatternProcessors(),
                getIntervalMillis());
    }

}
