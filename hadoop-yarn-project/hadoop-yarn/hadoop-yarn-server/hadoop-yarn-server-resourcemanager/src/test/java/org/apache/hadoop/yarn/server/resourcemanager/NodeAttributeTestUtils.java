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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Test Utils for NodeAttribute.
 */
public final class NodeAttributeTestUtils {

  private NodeAttributeTestUtils() {

  }

  public static YarnConfiguration getRandomDirConf(Configuration conf)
      throws IOException {
    YarnConfiguration newConf;
    if (conf == null) {
      newConf = new YarnConfiguration();
    } else {
      newConf = new YarnConfiguration(conf);
    }
    File tempDir = GenericTestUtils.getRandomizedTestDir();
    FileUtils.deleteDirectory(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    newConf.set(YarnConfiguration.FS_NODE_ATTRIBUTE_STORE_ROOT_DIR,
        tempDir.getAbsolutePath());
    return newConf;
  }

}
