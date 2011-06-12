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
package org.apache.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

/**
 * Test provides some very generic helpers which might be used across the tests
 */
public abstract class GenericTestUtils {
  /**
   * Extracts the name of the method where the invocation has happened
   * @return String name of the invoking method
   */
  public static String getMethodName() {
    return Thread.currentThread().getStackTrace()[2].getMethodName();
  }
  
  /**
   * when formating a namenode - we must provide clusterid.
   * @param conf
   * @throws IOException
   */
  public static void formatNamenode(Configuration conf) throws IOException {
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.isEmpty())
      StartupOption.FORMAT.setClusterId("testClusterID");

    NameNode.format(conf);
  }
}
