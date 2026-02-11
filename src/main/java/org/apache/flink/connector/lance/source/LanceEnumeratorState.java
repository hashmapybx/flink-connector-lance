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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Checkpoint state for {@link LanceSplitEnumerator}.
 *
 * <p>Stores unassigned Splits, used for reassignment when recovering from a checkpoint.
 */
public class LanceEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** List of unassigned Splits */
    private final List<LanceSourceSplit> pendingSplits;

    /**
     * Create a LanceEnumeratorState.
     *
     * @param pendingSplits List of unassigned Splits
     */
    public LanceEnumeratorState(Collection<LanceSourceSplit> pendingSplits) {
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
    }

    /**
     * Get the list of unassigned Splits.
     *
     * @return Immutable list of Splits
     */
    public List<LanceSourceSplit> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public String toString() {
        return "LanceEnumeratorState{"
                + "pendingSplits=" + pendingSplits.size()
                + '}';
    }
}
