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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Mock NameNodeResourceChecker with resource availability flag which will be
 * used to simulate the Namenode resource status.
 */
public class MockNameNodeResourceChecker extends NameNodeResourceChecker {
  private volatile boolean hasResourcesAvailable = true;

  public MockNameNodeResourceChecker(Configuration conf) throws IOException {
    super(conf);
  }

  @Override
  public boolean hasAvailableDiskSpace() {
    return hasResourcesAvailable;
  }

  /**
   * Sets resource availability flag.
   *
   * @param resourceAvailable
   *          sets true if the resource is available otherwise sets to false
   */
  public void setResourcesAvailable(boolean resourceAvailable) {
    hasResourcesAvailable = resourceAvailable;
  }
}
