package org.apache.flink.cep.dynamic.coordinator;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public class DynamicCepOperatorCoordinatorSerdeUtils {
    public static byte[] readBytes(DataInputStream in, int serializedCheckpointSize) {
        return new byte[0];
    }

    public static void verifyCoordinatorSerdeVersion(DataInputStream in) {

    }

    public static void writeCoordinatorSerdeVersion(DataOutputStream out) {

    }
}
