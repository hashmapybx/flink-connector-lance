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
package org.apache.flink.connector.lance;

import org.apache.flink.core.io.InputSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lance data split.
 *
 * <p>Represents a Fragment in Lance dataset, used for parallel data reading.
 */
public class LanceSplit implements InputSplit, Serializable {

  private static final long serialVersionUID = 1L;

  /** Split number */
  private final int splitNumber;

  /** Fragment ID */
  private final int fragmentId;

  /** Dataset path */
  private final String datasetPath;

  /** Row count in Fragment (estimated) */
  private final long rowCount;

  /**
   * Create LanceSplit
   *
   * @param splitNumber Split number
   * @param fragmentId Fragment ID
   * @param datasetPath Dataset path
   * @param rowCount Row count
   */
  public LanceSplit(int splitNumber, int fragmentId, String datasetPath, long rowCount) {
    this.splitNumber = splitNumber;
    this.fragmentId = fragmentId;
    this.datasetPath = datasetPath;
    this.rowCount = rowCount;
  }

  @Override
  public int getSplitNumber() {
    return splitNumber;
  }

  /** Get Fragment ID */
  public int getFragmentId() {
    return fragmentId;
  }

  /** Get dataset path */
  public String getDatasetPath() {
    return datasetPath;
  }

  /** Get row count */
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LanceSplit that = (LanceSplit) o;
    return splitNumber == that.splitNumber
        && fragmentId == that.fragmentId
        && rowCount == that.rowCount
        && Objects.equals(datasetPath, that.datasetPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitNumber, fragmentId, datasetPath, rowCount);
  }

  @Override
  public String toString() {
    return "LanceSplit{"
        + "splitNumber="
        + splitNumber
        + ", fragmentId="
        + fragmentId
        + ", datasetPath='"
        + datasetPath
        + '\''
        + ", rowCount="
        + rowCount
        + '}';
  }
}
