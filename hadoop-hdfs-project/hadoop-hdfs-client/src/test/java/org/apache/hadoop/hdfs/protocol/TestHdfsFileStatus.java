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
package org.apache.hadoop.hdfs.protocol;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link HdfsFileStatus}.
 */
public class TestHdfsFileStatus {
  private boolean createFileStatus(boolean isdir) {
    HdfsFileStatus status = new HdfsFileStatus.Builder()
        .isdir(isdir)
        .build();

    return status instanceof HdfsLocatedFileStatus;
  }

  @Test
  public void testHdfsFileStatusBuild() {
    // listing directory
    assertFalse("Status of directory should not be " +
            "HdfsLocatedFileStatus",
        createFileStatus(true));

    // listing file when locations is null
    assertTrue("Status of file should be HdfsLocatedFileStatus",
        createFileStatus(false));
  }
}

