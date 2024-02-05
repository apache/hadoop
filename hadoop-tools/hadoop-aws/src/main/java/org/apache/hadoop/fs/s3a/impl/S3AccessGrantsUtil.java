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

package org.apache.hadoop.fs.s3a.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.store.LogExactlyOnce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;

import static org.apache.hadoop.fs.s3a.Constants.AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED;

public class S3AccessGrantsUtil {

  protected static final Logger LOG =
      LoggerFactory.getLogger(S3AccessGrantsUtil.class);

  private static final LogExactlyOnce LOG_S3AG_PLUGIN_INFO = new LogExactlyOnce(LOG);
  private static final String S3AG_PLUGIN_CLASSNAME =
      "software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin";

  /**
   * S3 Access Grants plugin availability.
   */
  private static final boolean S3AG_PLUGIN_FOUND = checkForS3AGPlugin();

  private static boolean checkForS3AGPlugin() {
    try {
      ClassLoader cl = DefaultS3ClientFactory.class.getClassLoader();
      cl.loadClass(S3AG_PLUGIN_CLASSNAME);
      LOG.debug("S3 Access Grants plugin class {} found", S3AG_PLUGIN_CLASSNAME);
      return true;
    } catch (Exception e) {
      LOG.debug("S3 Access Grants plugin class {} not found", S3AG_PLUGIN_CLASSNAME, e);
      return false;
    }
  }

  /**
   * Is the S3AG plugin available?
   * @return true if it was found in the classloader
   */
  private static synchronized boolean isS3AGPluginAvailable() {
    return S3AG_PLUGIN_FOUND;
  }

  public static <BuilderT extends S3BaseClientBuilder<BuilderT, ClientT>, ClientT> void
      applyS3AccessGrantsConfigurations(BuilderT builder, Configuration conf) {
    if (isS3AGPluginAvailable()) {
      boolean s3agFallbackEnabled = conf.getBoolean(
          AWS_S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED, false);
      S3AccessGrantsPlugin accessGrantsPlugin =
          S3AccessGrantsPlugin.builder().enableFallback(s3agFallbackEnabled).build();
      builder.addPlugin(accessGrantsPlugin);
      LOG_S3AG_PLUGIN_INFO.info("S3 Access Grants plugin is added to s3 client with fallback: {}", s3agFallbackEnabled);
    }
  }

}
