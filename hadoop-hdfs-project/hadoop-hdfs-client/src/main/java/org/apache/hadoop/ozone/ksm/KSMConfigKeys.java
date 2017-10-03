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

package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.ozone.OzoneAcl;
/**
 * KSM Constants.
 */
public final class KSMConfigKeys {
  /**
   * Never constructed.
   */
  private KSMConfigKeys() {
  }


  public static final String OZONE_KSM_HANDLER_COUNT_KEY =
      "ozone.ksm.handler.count.key";
  public static final int OZONE_KSM_HANDLER_COUNT_DEFAULT = 20;

  public static final String OZONE_KSM_ADDRESS_KEY =
      "ozone.ksm.address";
  public static final String OZONE_KSM_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_KSM_PORT_DEFAULT = 9862;

  public static final String OZONE_KSM_HTTP_ENABLED_KEY =
      "ozone.ksm.http.enabled";
  public static final String OZONE_KSM_HTTP_BIND_HOST_KEY =
      "ozone.ksm.http-bind-host";
  public static final String OZONE_KSM_HTTPS_BIND_HOST_KEY =
      "ozone.ksm.https-bind-host";
  public static final String OZONE_KSM_HTTP_ADDRESS_KEY =
      "ozone.ksm.http-address";
  public static final String OZONE_KSM_HTTPS_ADDRESS_KEY =
      "ozone.ksm.https-address";
  public static final String OZONE_KSM_KEYTAB_FILE =
      "ozone.ksm.keytab.file";
  public static final String OZONE_KSM_HTTP_BIND_HOST_DEFAULT = "0.0.0.0";
  public static final int OZONE_KSM_HTTP_BIND_PORT_DEFAULT = 9874;
  public static final int OZONE_KSM_HTTPS_BIND_PORT_DEFAULT = 9875;

  // LevelDB cache file uses an off-heap cache in LevelDB of 128 MB.
  public static final String OZONE_KSM_DB_CACHE_SIZE_MB =
      "ozone.ksm.db.cache.size.mb";
  public static final int OZONE_KSM_DB_CACHE_SIZE_DEFAULT = 128;

  public static final String OZONE_KSM_USER_MAX_VOLUME =
      "ozone.ksm.user.max.volume";
  public static final int OZONE_KSM_USER_MAX_VOLUME_DEFAULT = 1024;

  // KSM Default user/group permissions
  public static final String OZONE_KSM_USER_RIGHTS =
      "ozone.ksm.user.rights";
  public static final OzoneAcl.OzoneACLRights OZONE_KSM_USER_RIGHTS_DEFAULT =
      OzoneAcl.OzoneACLRights.READ_WRITE;

  public static final String OZONE_KSM_GROUP_RIGHTS =
      "ozone.ksm.group.rights";
  public static final OzoneAcl.OzoneACLRights OZONE_KSM_GROUP_RIGHTS_DEFAULT =
      OzoneAcl.OzoneACLRights.READ_WRITE;

  public static final String OZONE_KEY_DELETING_LIMIT_PER_TASK =
      "ozone.key.deleting.limit.per.task";
  public static final int OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT = 1000;
}
