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
package org.apache.hadoop.hdds.discovery;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to download ozone configuration from SCM.
 */
public final class DiscoveryUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(DiscoveryUtil.class);

  public static final String OZONE_GLOBAL_XML = "ozone-global.xml";

  private DiscoveryUtil() {
  }

  /**
   * Download ozone-global.conf from SCM to the local HADOOP_CONF_DIR.
   */
  public static boolean loadGlobalConfig(OzoneConfiguration conf) {
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir == null || hadoopConfDir.isEmpty()) {
      LOG.warn(
          "HADOOP_CONF_DIR is not set, can't download ozone-global.xml from "
              + "SCM.");
      return false;
    }
    if (conf.get("ozone.scm.names") == null) {
      LOG.warn("ozone.scm.names is not set. Can't download config from scm.");
      return false;
    }
    for (int i = 0; i < 60; i++) {
      for (String scmHost : conf.getStrings("ozone.scm.names")) {
        String configOrigin =
            String.format("http://%s:9876/discovery/config", scmHost);
        File destinationFile = new File(hadoopConfDir, OZONE_GLOBAL_XML);

        try {
          LOG.info("Downloading {} to {}", configOrigin,
              destinationFile.getAbsolutePath());
          URL confUrl = new URL(configOrigin);
          ReadableByteChannel rbc = Channels.newChannel(confUrl.openStream());
          FileOutputStream fos =
              new FileOutputStream(
                  destinationFile);
          fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
          return true;
        } catch (Exception ex) {
          LOG.error("Can't download config from " + configOrigin, ex);
        }
      }
      LOG.warn(
          "Configuration download was unsuccessful. Let's wait 5 seconds and"
              + " retry.");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.error("Polling the config file upload is interrupted", e);
      }
    }
    return false;
  }
}
