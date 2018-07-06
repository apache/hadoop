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

import org.apache.hadoop.ozone.OzoneAcl;
/**
 * Ozone Manager Constants.
 */
public final class OMConfigKeys {
  /**
   * Never constructed.
   */
  private OMConfigKeys() {
  }


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
}
