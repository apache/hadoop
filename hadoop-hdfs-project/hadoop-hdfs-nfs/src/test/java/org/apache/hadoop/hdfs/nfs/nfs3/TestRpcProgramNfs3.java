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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
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

  @Test
  public void testDeprecatedKeys() {
    NfsConfiguration conf = new NfsConfiguration();
    conf.setInt("nfs3.server.port", 998);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY, 0) == 998);

    conf.setInt("nfs3.mountd.port", 999);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_MOUNTD_PORT_KEY, 0) == 999);

    conf.set("dfs.nfs.exports.allowed.hosts", "host1");
    assertTrue(conf.get(CommonConfigurationKeys.NFS_EXPORTS_ALLOWED_HOSTS_KEY)
        .equals("host1"));

    conf.setInt("dfs.nfs.exports.cache.expirytime.millis", 1000);
    assertTrue(conf.getInt(
        Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_KEY, 0) == 1000);

    conf.setInt("hadoop.nfs.userupdate.milly", 10);
    assertTrue(conf.getInt(Nfs3Constant.NFS_USERGROUP_UPDATE_MILLIS_KEY, 0) == 10);

    conf.set("dfs.nfs3.dump.dir", "/nfs/tmp");
    assertTrue(conf.get(NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_KEY).equals(
        "/nfs/tmp"));

    conf.setBoolean("dfs.nfs3.enableDump", false);
    assertTrue(conf.getBoolean(NfsConfigKeys.DFS_NFS_FILE_DUMP_KEY, true) == false);

    conf.setInt("dfs.nfs3.max.open.files", 500);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_KEY, 0) == 500);

    conf.setInt("dfs.nfs3.stream.timeout", 6000);
    assertTrue(conf.getInt(NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_KEY, 0) == 6000);

    conf.set("dfs.nfs3.export.point", "/dir1");
    assertTrue(conf.get(NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY).equals("/dir1"));
  }
}
