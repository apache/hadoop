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
package org.apache.hadoop.hdfs.client;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The public utility API for HDFS.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsUtils {
  public static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);

  /**
   * Is the HDFS healthy?
   * HDFS is considered as healthy if it is up and not in safemode.
   *
   * @param uri the HDFS URI.  Note that the URI path is ignored.
   * @return true if HDFS is healthy; false, otherwise.
   */
  public static boolean isHealthy(URI uri) {
    //check scheme
    final String scheme = uri.getScheme();
    if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("The scheme is not "
          + HdfsConstants.HDFS_URI_SCHEME + ", uri=" + uri);
    }

    final Configuration conf = new Configuration();
    //disable FileSystem cache
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", scheme), true);
    //disable client retry for rpc connection and rpc calls
    conf.setBoolean(HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY, false);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);

    DistributedFileSystem fs = null;
    try {
      fs = (DistributedFileSystem)FileSystem.get(uri, conf);
      final boolean safemode = fs.setSafeMode(SafeModeAction.SAFEMODE_GET);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Is namenode in safemode? " + safemode + "; uri=" + uri);
      }

      fs.close();
      fs = null;
      return !safemode;
    } catch(IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got an exception for uri=" + uri, e);
      }
      return false;
    } finally {
      IOUtils.closeQuietly(fs);
    }
  }
}
