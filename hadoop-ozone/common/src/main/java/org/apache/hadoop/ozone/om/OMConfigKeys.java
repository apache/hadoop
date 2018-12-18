/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.ratis.util.TimeDuration;

/**
 * Ozone Manager Constants.
 */
public final class OMConfigKeys {
  /**
   * Never constructed.
   */
  private OMConfigKeys() {
  }

  // Location where the OM stores its DB files. In the future we may support
  // multiple entries for performance (sharding)..
  public static final String OZONE_OM_DB_DIRS = "ozone.om.db.dirs";

  public static final String OZONE_OM_HANDLER_COUNT_KEY =
      "ozone.om.handler.count.key";
  public static final int OZONE_OM_HANDLER_COUNT_DEFAULT = 20;

  public static final String OZONE_OM_ADDRESS_KEY =
      "ozone.om.address";
  public static final String OZONE_OM_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_OM_PORT_DEFAULT = 9862;

  public static final String OZONE_OM_HTTP_ENABLED_KEY =
      "ozone.om.http.enabled";
  public static final String OZONE_OM_HTTP_BIND_HOST_KEY =
      "ozone.om.http-bind-host";
  public static final String OZONE_OM_HTTPS_BIND_HOST_KEY =
      "ozone.om.https-bind-host";
  public static final String OZONE_OM_HTTP_ADDRESS_KEY =
      "ozone.om.http-address";
  public static final String OZONE_OM_HTTPS_ADDRESS_KEY =
      "ozone.om.https-address";
  public static final String OZONE_OM_KEYTAB_FILE =
      "ozone.om.keytab.file";
  public static final String OZONE_OM_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_OM_HTTP_BIND_PORT_DEFAULT = 9874;
  public static final int OZONE_OM_HTTPS_BIND_PORT_DEFAULT = 9875;

  // LevelDB cache file uses an off-heap cache in LevelDB of 128 MB.
  public static final String OZONE_OM_DB_CACHE_SIZE_MB =
      "ozone.om.db.cache.size.mb";
  public static final int OZONE_OM_DB_CACHE_SIZE_DEFAULT = 128;

  public static final String OZONE_OM_USER_MAX_VOLUME =
      "ozone.om.user.max.volume";
  public static final int OZONE_OM_USER_MAX_VOLUME_DEFAULT = 1024;

  // OM Default user/group permissions
  public static final String OZONE_OM_USER_RIGHTS =
      "ozone.om.user.rights";
  public static final OzoneAcl.OzoneACLRights OZONE_OM_USER_RIGHTS_DEFAULT =
      OzoneAcl.OzoneACLRights.READ_WRITE;

  public static final String OZONE_OM_GROUP_RIGHTS =
      "ozone.om.group.rights";
  public static final OzoneAcl.OzoneACLRights OZONE_OM_GROUP_RIGHTS_DEFAULT =
      OzoneAcl.OzoneACLRights.READ_WRITE;

  public static final String OZONE_KEY_DELETING_LIMIT_PER_TASK =
      "ozone.key.deleting.limit.per.task";
  public static final int OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT = 1000;

  public static final String OZONE_OM_METRICS_SAVE_INTERVAL =
      "ozone.om.save.metrics.interval";
  public static final String OZONE_OM_METRICS_SAVE_INTERVAL_DEFAULT = "5m";

  /**
   * OM Ratis related configurations.
   */
  public static final String OZONE_OM_RATIS_ENABLE_KEY
      = "ozone.om.ratis.enable";
  public static final boolean OZONE_OM_RATIS_ENABLE_DEFAULT
      = false;
  public static final String OZONE_OM_RATIS_PORT_KEY
      = "ozone.om.ratis.port";
  public static final int OZONE_OM_RATIS_PORT_DEFAULT
      = 9872;
  // When set to true, allocate a random free port for ozone ratis server
  public static final String OZONE_OM_RATIS_RANDOM_PORT_KEY =
      "ozone.om.ratis.random.port";
  public static final boolean OZONE_OM_RATIS_RANDOM_PORT_KEY_DEFAULT
      = false;
  public static final String OZONE_OM_RATIS_RPC_TYPE_KEY
      = "ozone.om.ratis.rpc.type";
  public static final String OZONE_OM_RATIS_RPC_TYPE_DEFAULT
      = "GRPC";

  // OM Ratis Log configurations
  public static final String OZONE_OM_RATIS_STORAGE_DIR
      = "ozone.om.ratis.storage.dir";
  public static final String OZONE_OM_RATIS_SEGMENT_SIZE_KEY
      = "ozone.om.ratis.segment.size";
  public static final String OZONE_OM_RATIS_SEGMENT_SIZE_DEFAULT
      = "16KB";
  public static final String OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_KEY
      = "ozone.om.ratis.segment.preallocated.size";
  public static final String OZONE_OM_RATIS_SEGMENT_PREALLOCATED_SIZE_DEFAULT
      = "16KB";

  // OM Ratis Log Appender configurations
  public static final String
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS =
      "ozone.om.ratis.log.appender.queue.num-elements";
  public static final int
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS_DEFAULT = 1024;
  public static final String OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT =
      "ozone.om.ratis.log.appender.queue.byte-limit";
  public static final String
      OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT = "32MB";


  // OM Ratis server configurations
  public static final String OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_KEY
      = "ozone.om.ratis.server.request.timeout";
  public static final TimeDuration
      OZONE_OM_RATIS_SERVER_REQUEST_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
  public static final String
      OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_KEY
      = "ozone.om.ratis.server.retry.cache.timeout";
  public static final TimeDuration
      OZONE_OM_RATIS_SERVER_RETRY_CACHE_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
  public static final String OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY
      = "ozone.om.ratis.minimum.timeout";
  public static final TimeDuration OZONE_OM_RATIS_MINIMUM_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(1, TimeUnit.SECONDS);

  // OM Ratis client configurations
  public static final String OZONE_OM_RATIS_CLIENT_REQUEST_TIMEOUT_KEY
      = "ozone.om.ratis.client.request.timeout";
  public static final TimeDuration
      OZONE_OM_RATIS_CLIENT_REQUEST_TIMEOUT_DEFAULT
      = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
}
