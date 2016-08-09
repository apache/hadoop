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

package org.apache.slider.core.buildutils;

import org.apache.hadoop.util.VersionInfo;
import org.apache.slider.common.tools.SliderVersionInfo;

import java.util.Map;
import java.util.Properties;

/**
 * classes to help with the build
 */
public class BuildHelper {
  /**
   * Add the cluster build information; this will include Hadoop details too
   * @param dest map to insert this too
   * @param prefix prefix for the build info
   */
  public static void addBuildMetadata(Map<String, Object> dest, String prefix) {

    Properties props = SliderVersionInfo.loadVersionProperties();
    dest.put(prefix + "." + SliderVersionInfo.APP_BUILD_INFO,
             props.getProperty(
      SliderVersionInfo.APP_BUILD_INFO));
    dest.put(prefix + "." + SliderVersionInfo.HADOOP_BUILD_INFO,
             props.getProperty(SliderVersionInfo.HADOOP_BUILD_INFO));

    dest.put(prefix + "." + SliderVersionInfo.HADOOP_DEPLOYED_INFO,
             VersionInfo.getBranch() + " @" + VersionInfo.getSrcChecksum());
  }
}
