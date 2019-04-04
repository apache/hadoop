/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.csi;

/**
 * Configuration values for Ozone CSI daemon.
 */
public final class CsiConfigurationValues {

  private CsiConfigurationValues() {
  }

  public static final String OZONE_S3_ADDRESS =
      "ozone.s3g.address";

  public static final String OZONE_CSI_MOUNT_COMMAND =
      "ozone.csi.mount";

  public static final String OZONE_CSI_MOUNT_COMMAND_DEFAULT =
      "goofys --endpoint %s %s %s";

  public static final String OZONE_CSI_UMOUNT_COMMAND =
      "ozone.csi.umount";

  public static final String OZONE_CSI_UMOUNT_COMMAND_DEFAULT =
      "fusermount -u %s";

  public static final String OZONE_CSI_SOCKET =
      "ozone.csi.socket";

  public static final String OZONE_CSI_SOCKET_DEFAULT = "/tmp/csi.sock";

}
