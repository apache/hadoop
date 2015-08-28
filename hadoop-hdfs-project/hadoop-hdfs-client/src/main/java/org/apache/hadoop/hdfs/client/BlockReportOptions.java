/**
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
package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Options that can be specified when manually triggering a block report.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BlockReportOptions {
  private final boolean incremental;

  private BlockReportOptions(boolean incremental) {
    this.incremental = incremental;
  }

  public boolean isIncremental() {
    return incremental;
  }

  public static class Factory {
    private boolean incremental = false;

    public Factory() {
    }

    public Factory setIncremental(boolean incremental) {
      this.incremental = incremental;
      return this;
    }

    public BlockReportOptions build() {
      return new BlockReportOptions(incremental);
    }
  }

  @Override
  public String toString() {
    return "BlockReportOptions{incremental=" + incremental + "}";
  }
}
