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
    Object[][] procedures = {
        { Nfs3Constant.NFSPROC3.NULL, 1 },
        { Nfs3Constant.NFSPROC3.GETATTR, 1 },
        { Nfs3Constant.NFSPROC3.SETATTR, 1 },
        { Nfs3Constant.NFSPROC3.LOOKUP, 1 },
        { Nfs3Constant.NFSPROC3.ACCESS, 1 },
        { Nfs3Constant.NFSPROC3.READLINK, 1 },
        { Nfs3Constant.NFSPROC3.READ, 1 },
        { Nfs3Constant.NFSPROC3.WRITE, 1 },
        { Nfs3Constant.NFSPROC3.CREATE, 0 },
        { Nfs3Constant.NFSPROC3.MKDIR, 0 },
        { Nfs3Constant.NFSPROC3.SYMLINK, 0 },
        { Nfs3Constant.NFSPROC3.MKNOD, 0 },
        { Nfs3Constant.NFSPROC3.REMOVE, 0 },
        { Nfs3Constant.NFSPROC3.RMDIR, 0 },
        { Nfs3Constant.NFSPROC3.RENAME, 0 },
        { Nfs3Constant.NFSPROC3.LINK, 0 },
        { Nfs3Constant.NFSPROC3.READDIR, 1 },
        { Nfs3Constant.NFSPROC3.READDIRPLUS, 1 },
        { Nfs3Constant.NFSPROC3.FSSTAT, 1 },
        { Nfs3Constant.NFSPROC3.FSINFO, 1 },
        { Nfs3Constant.NFSPROC3.PATHCONF, 1 },
        { Nfs3Constant.NFSPROC3.COMMIT, 1 } };
    for (Object[] procedure : procedures) {
      boolean idempotent = procedure[1].equals(Integer.valueOf(1));
      Nfs3Constant.NFSPROC3 proc = (Nfs3Constant.NFSPROC3)procedure[0];
      if (idempotent) {
        Assert.assertTrue(("Procedure " + proc + " should be idempotent"),
            proc.isIdempotent());
      } else {
        Assert.assertFalse(("Procedure " + proc + " should be non-idempotent"),
            proc.isIdempotent());
      }
    }
  }
}
