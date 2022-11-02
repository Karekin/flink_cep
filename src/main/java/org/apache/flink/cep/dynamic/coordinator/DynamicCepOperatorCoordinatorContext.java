package org.apache.flink.cep.dynamic.coordinator;

import java.io.Closeable;
import java.io.IOException;

public class DynamicCepOperatorCoordinatorContext extends Closeable {
    public ClassLoader getUserCodeClassloader() {
        return null;
    }

    public void failJob(Throwable t) {

    }

    @Override
    public void close() throws IOException {

    }
}
