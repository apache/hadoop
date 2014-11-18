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

package org.apache.hadoop.hdfs.nfs.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.security.IdMappingConstant;

/**
 * Adds deprecated keys into the configuration.
 */
public class NfsConfiguration extends HdfsConfiguration {
  static {
    addDeprecatedKeys();
  }

  private static void addDeprecatedKeys() {
    Configuration.addDeprecations(new DeprecationDelta[] {
        new DeprecationDelta("nfs3.server.port",
            NfsConfigKeys.DFS_NFS_SERVER_PORT_KEY),
        new DeprecationDelta("nfs3.mountd.port",
            NfsConfigKeys.DFS_NFS_MOUNTD_PORT_KEY),
        new DeprecationDelta("dfs.nfs.exports.cache.size",
            Nfs3Constant.NFS_EXPORTS_CACHE_SIZE_KEY),
        new DeprecationDelta("dfs.nfs.exports.cache.expirytime.millis",
            Nfs3Constant.NFS_EXPORTS_CACHE_EXPIRYTIME_MILLIS_KEY),
        new DeprecationDelta("hadoop.nfs.userupdate.milly",
            IdMappingConstant.USERGROUPID_UPDATE_MILLIS_KEY),
        new DeprecationDelta("nfs.usergroup.update.millis",
            IdMappingConstant.USERGROUPID_UPDATE_MILLIS_KEY),
        new DeprecationDelta("nfs.static.mapping.file",
            IdMappingConstant.STATIC_ID_MAPPING_FILE_KEY),
        new DeprecationDelta("dfs.nfs3.enableDump",
            NfsConfigKeys.DFS_NFS_FILE_DUMP_KEY),
        new DeprecationDelta("dfs.nfs3.dump.dir",
            NfsConfigKeys.DFS_NFS_FILE_DUMP_DIR_KEY),
        new DeprecationDelta("dfs.nfs3.max.open.files",
            NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_KEY),
        new DeprecationDelta("dfs.nfs3.stream.timeout",
            NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_KEY),
        new DeprecationDelta("dfs.nfs3.export.point",
            NfsConfigKeys.DFS_NFS_EXPORT_POINT_KEY),
        new DeprecationDelta("nfs.allow.insecure.ports",
            NfsConfigKeys.DFS_NFS_PORT_MONITORING_DISABLED_KEY),
        new DeprecationDelta("dfs.nfs.keytab.file",
            NfsConfigKeys.DFS_NFS_KEYTAB_FILE_KEY),
        new DeprecationDelta("dfs.nfs.kerberos.principal",
            NfsConfigKeys.DFS_NFS_KERBEROS_PRINCIPAL_KEY),
        new DeprecationDelta("dfs.nfs.rtmax",
            NfsConfigKeys.DFS_NFS_MAX_READ_TRANSFER_SIZE_KEY),
        new DeprecationDelta("dfs.nfs.wtmax",
            NfsConfigKeys.DFS_NFS_MAX_WRITE_TRANSFER_SIZE_KEY),
        new DeprecationDelta("dfs.nfs.dtmax",
            NfsConfigKeys.DFS_NFS_MAX_READDIR_TRANSFER_SIZE_KEY) });
  }
}