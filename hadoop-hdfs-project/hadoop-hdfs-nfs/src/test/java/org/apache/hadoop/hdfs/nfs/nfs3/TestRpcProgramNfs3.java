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
package org.apache.hadoop.hdfs.nfs.nfs3;

import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link RpcProgramNfs3}
 */
public class TestRpcProgramNfs3 {
  @Test(timeout=1000)
  public void testIdempotent() {
    int[][] procedures = {
        { Nfs3Constant.NFSPROC3_NULL, 1 },
        { Nfs3Constant.NFSPROC3_GETATTR, 1 },
        { Nfs3Constant.NFSPROC3_SETATTR, 1 },
        { Nfs3Constant.NFSPROC3_LOOKUP, 1 },
        { Nfs3Constant.NFSPROC3_ACCESS, 1 },
        { Nfs3Constant.NFSPROC3_READLINK, 1 },
        { Nfs3Constant.NFSPROC3_READ, 1 },
        { Nfs3Constant.NFSPROC3_WRITE, 1 },
        { Nfs3Constant.NFSPROC3_CREATE, 0 },
        { Nfs3Constant.NFSPROC3_MKDIR, 0 },
        { Nfs3Constant.NFSPROC3_SYMLINK, 0 },
        { Nfs3Constant.NFSPROC3_MKNOD, 0 },
        { Nfs3Constant.NFSPROC3_REMOVE, 0 },
        { Nfs3Constant.NFSPROC3_RMDIR, 0 },
        { Nfs3Constant.NFSPROC3_RENAME, 0 },
        { Nfs3Constant.NFSPROC3_LINK, 0 },
        { Nfs3Constant.NFSPROC3_READDIR, 1 },
        { Nfs3Constant.NFSPROC3_READDIRPLUS, 1 },
        { Nfs3Constant.NFSPROC3_FSSTAT, 1 },
        { Nfs3Constant.NFSPROC3_FSINFO, 1 },
        { Nfs3Constant.NFSPROC3_PATHCONF, 1 },
        { Nfs3Constant.NFSPROC3_COMMIT, 1 } };
    for (int[] procedure : procedures) {
      boolean idempotent = procedure[1] == 1;
      int proc = procedure[0];
      if (idempotent) {
        Assert.assertTrue(("Procedure " + proc + " should be idempotent"),
            RpcProgramNfs3.isIdempotent(proc));
      } else {
        Assert.assertFalse(("Procedure " + proc + " should be non-idempotent"),
            RpcProgramNfs3.isIdempotent(proc));
      }
    }
  }
}
