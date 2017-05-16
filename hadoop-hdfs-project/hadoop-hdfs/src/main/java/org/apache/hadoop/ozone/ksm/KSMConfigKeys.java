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
      "ozone.scm.handler.count.key";
  public static final int OZONE_KSM_HANDLER_COUNT_DEFAULT = 200;

  public static final String OZONE_KSM_ADDRESS_KEY =
      "ozone.ksm.address";
  public static final String OZONE_KSM_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_KSM_PORT_DEFAULT = 9862;

  // LevelDB cache file uses an off-heap cache in LevelDB of 128 MB.
  public static final String OZONE_KSM_DB_CACHE_SIZE_MB =
      "ozone.ksm.leveldb.cache.size.mb";
  public static final int OZONE_KSM_DB_CACHE_SIZE_DEFAULT = 128;

  public static final String OZONE_KSM_USER_MAX_VOLUME =
      "ozone.ksm.user.max.volume";
  public static final int OZONE_KSM_USER_MAX_VOLUME_DEFAULT = 1024;
}
