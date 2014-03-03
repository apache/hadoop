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

package org.apache.hadoop.yarn;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

@Private
@Unstable
public class FileSystemBasedConfigurationProvider
    extends ConfigurationProvider {

  private static final Log LOG = LogFactory
      .getLog(FileSystemBasedConfigurationProvider.class);
  private FileSystem fs;
  private Path configDir;

  @Override
  public synchronized InputStream getConfigurationInputStream(
      Configuration bootstrapConf, String name) throws IOException,
      YarnException {
    if (name == null || name.isEmpty()) {
      throw new YarnException(
          "Illegal argument! The parameter should not be null or empty");
    }
    Path filePath;
    if (YarnConfiguration.RM_CONFIGURATION_FILES.contains(name)) {
      filePath = new Path(this.configDir, name);
      if (!fs.exists(filePath)) {
        LOG.info(filePath + " not found");
        return null;
      }
    } else {
      filePath = new Path(name);
      if (!fs.exists(filePath)) {
        LOG.info(filePath + " not found");
        return null;
      }
    }
    return fs.open(filePath);
  }

  @Override
  public synchronized void initInternal(Configuration bootstrapConf)
      throws Exception {
    configDir =
        new Path(bootstrapConf.get(YarnConfiguration.FS_BASED_RM_CONF_STORE,
            YarnConfiguration.DEFAULT_FS_BASED_RM_CONF_STORE));
    fs = configDir.getFileSystem(bootstrapConf);
    if (!fs.exists(configDir)) {
      fs.mkdirs(configDir);
    }
  }

  @Override
  public synchronized void closeInternal() throws Exception {
    fs.close();
  }
}
