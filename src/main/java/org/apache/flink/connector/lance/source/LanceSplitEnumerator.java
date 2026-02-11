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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.lance.config.LanceOptions;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Split coordinator for Lance Source.
 *
 * <p>Discovers all Fragments in a Lance Dataset and assigns them as Splits to SourceReaders.
 * Similar to the SplitManager role in Trino.
 *
 * <p>Main responsibilities:
 * <ul>
 *   <li>Open Dataset and enumerate all Fragments</li>
 *   <li>Wrap Fragments as {@link LanceSourceSplit}</li>
 *   <li>Respond to SourceReader split requests and assign on demand</li>
 *   <li>Support checkpoint and recovery</li>
 * </ul>
 */
public class LanceSplitEnumerator implements SplitEnumerator<LanceSourceSplit, LanceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSplitEnumerator.class);

    private final SplitEnumeratorContext<LanceSourceSplit> context;
    private final LanceOptions options;

    /** Queue of pending Splits to be assigned */
    private final Queue<LanceSourceSplit> pendingSplits;

    /** Set of registered reader IDs */
    private final java.util.Set<Integer> registeredReaders;

    /** Whether split discovery has finished */
    private boolean splitDiscoveryFinished;

    /**
     * Create a new LanceSplitEnumerator.
     *
     * @param context Enumerator context
     * @param options Lance configuration
     */
    public LanceSplitEnumerator(
            SplitEnumeratorContext<LanceSourceSplit> context,
            LanceOptions options) {
        this(context, options, new ArrayList<>());
    }

    /**
     * Create a LanceSplitEnumerator restored from checkpoint.
     *
     * @param context       Enumerator context
     * @param options       Lance configuration
     * @param pendingSplits Recovered pending Splits
     */
    public LanceSplitEnumerator(
            SplitEnumeratorContext<LanceSourceSplit> context,
            LanceOptions options,
            Collection<LanceSourceSplit> pendingSplits) {
        this.context = context;
        this.options = options;
        this.pendingSplits = new ArrayDeque<>(pendingSplits);
        this.registeredReaders = new java.util.HashSet<>();
        this.splitDiscoveryFinished = !pendingSplits.isEmpty();
    }

    @Override
    public void start() {
        LOG.info("Starting LanceSplitEnumerator, dataset path: {}", options.getPath());
        if (!splitDiscoveryFinished) {
            context.callAsync(this::discoverSplits, this::handleSplitDiscovery);
        }
    }

    /**
     * Discover all Splits (executed in async thread).
     */
    private List<LanceSourceSplit> discoverSplits() {
        LOG.info("Starting to discover Lance Dataset Fragments...");

        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new RuntimeException("Lance dataset path must not be empty");
        }

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        try {
            Dataset dataset = Dataset.open(datasetPath, allocator);
            try {
                List<Fragment> fragments = dataset.getFragments();
                List<LanceSourceSplit> splits = new ArrayList<>(fragments.size());

                for (Fragment fragment : fragments) {
                    long rowCount = fragment.countRows();
                    splits.add(new LanceSourceSplit(fragment.getId(), datasetPath, rowCount));
                }

                LOG.info("Discovered {} Fragments, total rows: {}",
                        splits.size(),
                        splits.stream().mapToLong(LanceSourceSplit::getRowCount).sum());

                return splits;
            } finally {
                dataset.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to open Lance Dataset: " + datasetPath, e);
        } finally {
            allocator.close();
        }
    }

    /**
     * Handle split discovery result (executed in main thread).
     */
    private void handleSplitDiscovery(List<LanceSourceSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Error during split discovery", error);
            throw new RuntimeException("Split discovery failed", error);
        }

        pendingSplits.addAll(splits);
        splitDiscoveryFinished = true;

        LOG.info("Split discovery completed, {} pending Splits", pendingSplits.size());

        // Assign Splits to all registered readers
        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Received split request from subtask {}", subtaskId);

        if (!pendingSplits.isEmpty()) {
            LanceSourceSplit split = pendingSplits.poll();
            if (split != null) {
                LOG.info("Assigning Split {} to subtask {}", split.splitId(), subtaskId);
                List<LanceSourceSplit> assignment = new ArrayList<>();
                assignment.add(split);
                context.assignSplits(new org.apache.flink.api.connector.source.SplitsAssignment<>(
                        java.util.Collections.singletonMap(subtaskId, assignment)));
            }
        } else if (splitDiscoveryFinished) {
            // All Splits have been assigned, notify Reader that there are no more Splits
            LOG.info("All Splits assigned, notifying subtask {} no more Splits", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
        // If split discovery hasn't finished yet, do nothing; splits will be assigned after discovery
    }

    @Override
    public void addSplitsBack(List<LanceSourceSplit> splits, int subtaskId) {
        LOG.info("Subtask {} returned {} Splits", subtaskId, splits.size());
        pendingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Reader {} registered", subtaskId);
        registeredReaders.add(subtaskId);
        // When reader registers, assign pending splits immediately if available
        if (splitDiscoveryFinished && !pendingSplits.isEmpty()) {
            assignSplitToReader(subtaskId);
        }
    }

    @Override
    public LanceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Checkpoint {} snapshot, pending Splits: {}", checkpointId, pendingSplits.size());
        return new LanceEnumeratorState(new ArrayList<>(pendingSplits));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing LanceSplitEnumerator");
    }

    /**
     * Assign pending Splits to all registered readers.
     */
    private void assignPendingSplits() {
        // Only assign Splits to registered readers
        for (Integer readerId : registeredReaders) {
            if (pendingSplits.isEmpty()) {
                break;
            }
            assignSplitToReader(readerId);
        }
    }

    /**
     * Assign a single Split to the specified reader.
     */
    private void assignSplitToReader(int subtaskId) {
        if (pendingSplits.isEmpty()) {
            if (splitDiscoveryFinished) {
                context.signalNoMoreSplits(subtaskId);
            }
            return;
        }

        LanceSourceSplit split = pendingSplits.poll();
        if (split != null) {
            Map<Integer, List<LanceSourceSplit>> assignment = new HashMap<>();
            List<LanceSourceSplit> splitList = new ArrayList<>();
            splitList.add(split);
            assignment.put(subtaskId, splitList);

            LOG.info("Assigning Split {} to subtask {}", split.splitId(), subtaskId);
            context.assignSplits(new org.apache.flink.api.connector.source.SplitsAssignment<>(assignment));
        }
    }
}
