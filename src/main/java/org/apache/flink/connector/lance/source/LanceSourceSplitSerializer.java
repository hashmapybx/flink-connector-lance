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

package org.apache.flink.connector.lance.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serializer for {@link LanceSourceSplit}.
 *
 * <p>Used for serializing/deserializing Splits during checkpoint and recovery.
 */
public class LanceSourceSplitSerializer implements SimpleVersionedSerializer<LanceSourceSplit> {

    public static final LanceSourceSplitSerializer INSTANCE = new LanceSourceSplitSerializer();

    private static final int CURRENT_VERSION = 1;

    private LanceSourceSplitSerializer() {
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(LanceSourceSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        out.writeInt(split.getFragmentId());
        out.writeUTF(split.getDatasetPath());
        out.writeLong(split.getRowCount());

        out.flush();
        return baos.toByteArray();
    }

    @Override
    public LanceSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported serialization version: " + version + ", current version: " + CURRENT_VERSION);
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));

        int fragmentId = in.readInt();
        String datasetPath = in.readUTF();
        long rowCount = in.readLong();

        return new LanceSourceSplit(fragmentId, datasetPath, rowCount);
    }
}
