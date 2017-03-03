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

public class NfsConfigKeys {

  // The IP port number for NFS and mountd.
  public final static String DFS_NFS_SERVER_PORT_KEY = "nfs.server.port";
  public final static int DFS_NFS_SERVER_PORT_DEFAULT = 2049;
  public final static String DFS_NFS_MOUNTD_PORT_KEY = "nfs.mountd.port";
  public final static int DFS_NFS_MOUNTD_PORT_DEFAULT = 4242;
  
  public static final String DFS_NFS_FILE_DUMP_KEY = "nfs.file.dump";
  public static final boolean DFS_NFS_FILE_DUMP_DEFAULT = true;
  public static final String DFS_NFS_FILE_DUMP_DIR_KEY = "nfs.dump.dir";
  public static final String DFS_NFS_FILE_DUMP_DIR_DEFAULT = "/tmp/.hdfs-nfs";
  
  public static final String DFS_NFS_MAX_READ_TRANSFER_SIZE_KEY = "nfs.rtmax";
  public static final int DFS_NFS_MAX_READ_TRANSFER_SIZE_DEFAULT = 1024 * 1024;
  public static final String DFS_NFS_MAX_WRITE_TRANSFER_SIZE_KEY = "nfs.wtmax";
  public static final int DFS_NFS_MAX_WRITE_TRANSFER_SIZE_DEFAULT = 1024 * 1024;
  public static final String DFS_NFS_MAX_READDIR_TRANSFER_SIZE_KEY = "nfs.dtmax";
  public static final int DFS_NFS_MAX_READDIR_TRANSFER_SIZE_DEFAULT = 64 * 1024;

  public static final String DFS_NFS_MAX_OPEN_FILES_KEY = "nfs.max.open.files";
  public static final int DFS_NFS_MAX_OPEN_FILES_DEFAULT = 256;

  public static final String DFS_NFS_STREAM_TIMEOUT_KEY = "nfs.stream.timeout";
  public static final long DFS_NFS_STREAM_TIMEOUT_DEFAULT = 10 * 60 * 1000; // 10 minutes
  public static final long DFS_NFS_STREAM_TIMEOUT_MIN_DEFAULT = 10 * 1000; // 10 seconds

  public final static String DFS_NFS_EXPORT_POINT_KEY = "nfs.export.point";
  public final static String DFS_NFS_EXPORT_POINT_DEFAULT = "/";
  
  public static final String DFS_NFS_KEYTAB_FILE_KEY = "nfs.keytab.file";
  public static final String DFS_NFS_KERBEROS_PRINCIPAL_KEY = "nfs.kerberos.principal";
  public static final String DFS_NFS_REGISTRATION_PORT_KEY = "nfs.registration.port";
  public static final int DFS_NFS_REGISTRATION_PORT_DEFAULT = 40; // Currently unassigned.
  public static final String DFS_NFS_PORT_MONITORING_DISABLED_KEY = "nfs.port.monitoring.disabled";
  public static final boolean DFS_NFS_PORT_MONITORING_DISABLED_DEFAULT = true;

  public static final String  AIX_COMPAT_MODE_KEY = "nfs.aix.compatibility.mode.enabled";
  public static final boolean AIX_COMPAT_MODE_DEFAULT = false;
  
  public final static String LARGE_FILE_UPLOAD = "nfs.large.file.upload";
  public final static boolean LARGE_FILE_UPLOAD_DEFAULT = true;
  
  public static final String NFS_HTTP_PORT_KEY = "nfs.http.port";
  public static final int NFS_HTTP_PORT_DEFAULT = 50079;
  public static final String NFS_HTTP_ADDRESS_KEY = "nfs.http.address";
  public static final String NFS_HTTP_ADDRESS_DEFAULT = "0.0.0.0:" + NFS_HTTP_PORT_DEFAULT;

  public static final String NFS_HTTPS_PORT_KEY = "nfs.https.port";
  public static final int NFS_HTTPS_PORT_DEFAULT = 50579;
  public static final String NFS_HTTPS_ADDRESS_KEY = "nfs.https.address";
  public static final String NFS_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:" + NFS_HTTPS_PORT_DEFAULT;
  
  public static final String  NFS_METRICS_PERCENTILES_INTERVALS_KEY = "nfs.metrics.percentiles.intervals";
  
  /*
   * HDFS super-user is the user with the same identity as NameNode process
   * itself and the super-user can do anything in that permissions checks never
   * fail for the super-user. If the following property is configured, the
   * superuser on NFS client can access any file on HDFS. By default, the super
   * user is not configured in the gateway. Note that, even the the superuser is
   * configured, "nfs.exports.allowed.hosts" still takes effect. For example,
   * the superuser will not have write access to HDFS files through the gateway
   * if the NFS client host is not allowed to have write access in
   * "nfs.exports.allowed.hosts".
   */
  public static final String  NFS_SUPERUSER_KEY = "nfs.superuser";
  public static final String  NFS_SUPERUSER_DEFAULT = "";
  /*
   * Timeout value in millisecond for rpc connection to portmap
   */
  public static final String NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_KEY =
                                      "nfs.udp.client.portmap.timeout.millis";
  public static final int NFS_UDP_CLIENT_PORTMAP_TIMEOUT_MILLIS_DEFAULT = 500;
}
