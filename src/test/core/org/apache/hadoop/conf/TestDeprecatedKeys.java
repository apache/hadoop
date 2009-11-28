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

package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import junit.framework.TestCase;

public class TestDeprecatedKeys extends TestCase {
 
  //Tests a deprecated key
  public void testDeprecatedKeys() throws Exception {
    Configuration conf = new Configuration();
    conf.set("topology.script.file.name", "xyz");
    String scriptFile = conf.get(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY);
    assertTrue(scriptFile.equals("xyz")) ;
    int m = conf.getInt(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT) ;
    assertTrue(m == 0022) ;
    conf.setInt("dfs.umask", 0077);
    m = conf.getInt(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT) ;
    assertTrue(m == 0077) ;
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "405");
    String umask = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    assertTrue(umask.equals("405"));
  }
}
