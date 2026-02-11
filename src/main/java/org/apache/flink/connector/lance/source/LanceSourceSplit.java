/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lance Source V2 data split.
 *
 * <p>Represents a Fragment in a Lance Dataset, used for parallel data reading. Each Split
 * corresponds to a Fragment, assigned by {@link LanceSplitEnumerator} to {@link LanceSourceReader}.
 *
 * <p>This class is immutable; all fields cannot be modified after construction.
 */
public class LanceSourceSplit implements SourceSplit, Serializable {

  private static final long serialVersionUID = 1L;

  /** Fragment ID */
  private final int fragmentId;

  /** Dataset path */
  private final String datasetPath;

  /** Estimated row count in the Fragment */
  private final long rowCount;

  /**
   * Create a LanceSourceSplit.
   *
   * @param fragmentId Fragment ID
   * @param datasetPath Dataset path
   * @param rowCount Row count
   */
  public LanceSourceSplit(int fragmentId, String datasetPath, long rowCount) {
    this.fragmentId = fragmentId;
    this.datasetPath = Objects.requireNonNull(datasetPath, "datasetPath must not be null");
    this.rowCount = rowCount;
  }

  @Override
  public String splitId() {
    return "lance-split-" + fragmentId;
  }

  /** Get Fragment ID. */
  public int getFragmentId() {
    return fragmentId;
  }

  /** Get Dataset path. */
  public String getDatasetPath() {
    return datasetPath;
  }

  /** Get row count. */
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceSourceSplit that = (LanceSourceSplit) o;
    return fragmentId == that.fragmentId
        && rowCount == that.rowCount
        && Objects.equals(datasetPath, that.datasetPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fragmentId, datasetPath, rowCount);
  }

  @Override
  public String toString() {
    return "LanceSourceSplit{"
        + "fragmentId="
        + fragmentId
        + ", datasetPath='"
        + datasetPath
        + '\''
        + ", rowCount="
        + rowCount
        + '}';
  }
}
