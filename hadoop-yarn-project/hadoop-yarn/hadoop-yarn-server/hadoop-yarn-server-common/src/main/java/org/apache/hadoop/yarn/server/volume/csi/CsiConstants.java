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
package org.apache.hadoop.yarn.server.volume.csi;

/**
 * CSI constants.
 */
public final class CsiConstants {

  private CsiConstants() {
    // Hide the constructor for this constant class.
  }

  public static final String CSI_VOLUME_NAME = "volume.name";
  public static final String CSI_VOLUME_ID = "volume.id";
  public static final String CSI_VOLUME_CAPABILITY = "volume.capability";
  public static final String CSI_DRIVER_NAME = "driver.name";
  public static final String CSI_VOLUME_MOUNT = "volume.mount";
  public static final String CSI_VOLUME_ACCESS_MODE =  "volume.accessMode";

  public static final String CSI_VOLUME_RESOURCE_TAG = "system:csi-volume";
}
