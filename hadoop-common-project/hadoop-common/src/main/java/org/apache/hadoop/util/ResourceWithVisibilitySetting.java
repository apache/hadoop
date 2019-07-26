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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The class is used to represent one resource(libjar, archives or files)
 * passed from cmd line. One resource require a path and optional
 * visibility setting {See @org.apache.hadoop.MRResourceVisibility}
 * E.g. -libjars example.jar::public or -libjars example.jar
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ResourceWithVisibilitySetting {
  public static final String PATH_SETTING_DELIMITER = "::";

  private String pathStr;
  private String visibilitySettings;

  private ResourceWithVisibilitySetting() {
  }

  public String getPathStr() {
    return pathStr;
  }

  public String getVisibilitySettings() {
    return visibilitySettings;
  }

  public ResourceWithVisibilitySetting(
      String pathString, String visibilitySettingsStr) {
    pathStr = pathString;
    visibilitySettings = visibilitySettingsStr;
  }

  /**
   * Serialize resource to string.
   * @return $fileName:$settings
   */
  public static String serialize(
      ResourceWithVisibilitySetting resourceWithVisibilitySetting) {
    if (resourceWithVisibilitySetting.visibilitySettings == null
        || resourceWithVisibilitySetting.visibilitySettings.isEmpty()) {
      return resourceWithVisibilitySetting.pathStr;
    }
    return resourceWithVisibilitySetting.pathStr + PATH_SETTING_DELIMITER
        + resourceWithVisibilitySetting.visibilitySettings;
  }

  /**
   * Deserialize resource from string, the format should be
   * {$fileName::$settings}.
   * @param resourceStr
   * @return ResourceWithVisibilitySetting
   */
  public static ResourceWithVisibilitySetting deserialize(String resourceStr) {
    String stripSchemaString = resourceStr;
    // hdfs://exmaple.jar
    if (!resourceStr.contains(PATH_SETTING_DELIMITER)) {
      return new ResourceWithVisibilitySetting(resourceStr, null);
    }
    // hdfs://exmaple.jar::public
    String[] strArr = stripSchemaString.split(PATH_SETTING_DELIMITER);
    if (strArr.length != 2) {
      throw new IllegalArgumentException(
          "Resource format should be $fileName::$settings, " + resourceStr);
    }
    // public of hdfs://exmaple.jar::public
    String settings = strArr[1];
    // hdfs://exmaple.jar
    String uriPath = resourceStr.substring(
        0, resourceStr.length()
            - PATH_SETTING_DELIMITER.length() - settings.length());
    return new ResourceWithVisibilitySetting(uriPath, settings);
  }
}
