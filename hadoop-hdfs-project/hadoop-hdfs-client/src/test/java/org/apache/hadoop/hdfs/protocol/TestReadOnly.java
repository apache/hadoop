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

import org.apache.hadoop.hdfs.server.namenode.ha.ReadOnly;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Testing class for {@link ReadOnly} annotation on {@link ClientProtocol}.
 */
public class TestReadOnly {
  private static final Method[] ALL_METHODS = ClientProtocol.class.getMethods();
  private static final Set<String> READONLY_METHOD_NAMES = new HashSet<>(
      Arrays.asList(
          "getBlockLocations",
          "getServerDefaults",
          "getStoragePolicies",
          "getStoragePolicy",
          "getListing",
          "getSnapshottableDirListing",
          "getPreferredBlockSize",
          "listCorruptFileBlocks",
          "getFileInfo",
          "isFileClosed",
          "getFileLinkInfo",
          "getLocatedFileInfo",
          "getContentSummary",
          "getLinkTarget",
          "getSnapshotDiffReport",
          "getSnapshotDiffReportListing",
          "listCacheDirectives",
          "listCachePools",
          "getAclStatus",
          "getEZForPath",
          "listEncryptionZones",
          "listReencryptionStatus",
          "getXAttrs",
          "listXAttrs",
          "checkAccess",
          "getErasureCodingPolicies",
          "getErasureCodingCodecs",
          "getErasureCodingPolicy",
          "listOpenFiles",
          "getStats",
          "getReplicatedBlockStats",
          "getECBlockGroupStats",
          "getDatanodeReport",
          "getDatanodeStorageReport",
          "getDataEncryptionKey",
          "getCurrentEditLogTxid",
          "getEditsFromTxid",
          "getQuotaUsage",
          "msync",
          "getHAServiceState"
      )
  );

  @Test
  public void testReadOnly() {
    for (Method m : ALL_METHODS) {
      boolean expected = READONLY_METHOD_NAMES.contains(m.getName());
      checkIsReadOnly(m.getName(), expected);
    }
  }

  private void checkIsReadOnly(String methodName, boolean expected) {
    for (Method m : ALL_METHODS) {
      // Note here we only check the FIRST result of overloaded methods
      // with the same name. The assumption is that all these methods should
      // share the same annotation.
      if (m.getName().equals(methodName)) {
        assertEquals("Expected ReadOnly for method '" + methodName +
            "' to be " + expected,
            m.isAnnotationPresent(ReadOnly.class), expected);
        return;
      }
    }
    throw new IllegalArgumentException("Unknown method name: " + methodName);
  }

}
