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
package org.apache.hadoop.utils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class returns build information about Hadoop components.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class VersionInfo {

  private final Properties info = new Properties();

  public VersionInfo(String component) {
    String versionInfoFile = component + "-version-info.properties";
    InputStream is = null;
    try {
      is = ThreadUtil.getResourceAsStream(
        getClass().getClassLoader(),
        versionInfoFile);
      info.load(is);
    } catch (IOException ex) {
      LoggerFactory.getLogger(getClass()).warn("Could not read '" +
          versionInfoFile + "', " + ex.toString(), ex);
    } finally {
      IOUtils.closeStream(is);
    }
  }

  public String getRelease() {
    return info.getProperty("release", "Unknown");
  }

  public String getVersion() {
    return info.getProperty("version", "Unknown");
  }

  public String getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  public String getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  public String getDate() {
    return info.getProperty("date", "Unknown");
  }

  public String getUser() {
    return info.getProperty("user", "Unknown");
  }

  public String getUrl() {
    return info.getProperty("url", "Unknown");
  }

  public String getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  public String getProtocVersion() {
    return info.getProperty("protocVersion", "Unknown");
  }

  public String getBuildVersion() {
    return getVersion() +
        " from " + getRevision() +
        " by " + getUser() +
        " source checksum " + getSrcChecksum();
  }
}
