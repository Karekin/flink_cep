package org.apache.flink.cep.dynamic.coordinator;

import org.apache.flink.cep.event.UpdatePatternProcessorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import java.io.Closeable;
import java.io.IOException;


public class DynamicCepOperatorCoordinatorContext implements Closeable {

    private DynamicCepOperatorCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory;
    private OperatorCoordinator.Context context;

    public DynamicCepOperatorCoordinatorContext(
            DynamicCepOperatorCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context context) {
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.context = context;
    }

    public ClassLoader getUserCodeClassloader() {
        return ClassLoader.getSystemClassLoader();
    }

    public void failJob(Throwable t) {

    }

    @Override
    public void close() throws IOException {

    }


    public <T> void sendEventToOperator(int subtask, UpdatePatternProcessorEvent<T> tUpdatePatternProcessorEvent) {

    }

    public void subtaskReady(OperatorCoordinator.SubtaskGateway gateway) {

    }

    public void subtaskNotReady(int subtask) {

    }

    public int[] getSubtasks() {
        return new int[0];
    }

    public void runInCoordinatorThread(Runnable runnable) {
        runnable.run();
    }
}
