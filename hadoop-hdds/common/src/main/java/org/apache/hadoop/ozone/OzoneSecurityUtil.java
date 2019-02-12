/*
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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Ozone security Util class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class OzoneSecurityUtil {

  private OzoneSecurityUtil() {
  }

  public static boolean isSecurityEnabled(Configuration conf) {
    return conf.getBoolean(OZONE_SECURITY_ENABLED_KEY,
        OZONE_SECURITY_ENABLED_DEFAULT);
  }

  /**
   * Returns Keys status.
   *
   * @return True if the key files exist.
   */
  public static boolean checkIfFileExist(Path path, String fileName) {
    if (Files.exists(path) && Files.exists(Paths.get(path.toString(),
        fileName))) {
      return true;
    }
    return false;
  }

}
