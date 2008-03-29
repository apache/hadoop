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
package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UnixUserGroupInformation;

public class TestSubmitJob extends junit.framework.TestCase 
{
  static final Path TEST_ROOT_DIR = new Path(
      System.getProperty("test.build.data", "/tmp")
      + Path.SEPARATOR + TestSubmitJob.class.getSimpleName());
  static final JobConf conf = new JobConf(TestSubmitJob.class);

  static JobConf createJobConf(FileSystem fs) throws Exception {
    fs.delete(TEST_ROOT_DIR);
    Path input = new Path(TEST_ROOT_DIR, "input");
    if (!fs.mkdirs(input)) {
      throw new Exception("Cannot create " + input);
    }
    conf.setInputPath(input);
    conf.setOutputPath(new Path(TEST_ROOT_DIR, "output"));
    return conf;    
  }
  
  public void testSubmitJobUsername() throws Exception {
    FileSystem.LOG.info("TEST_ROOT_DIR=" + TEST_ROOT_DIR);
    FileSystem fs = FileSystem.get(conf);
    try {
      UnixUserGroupInformation ugi = UnixUserGroupInformation.login(conf);
  
      JobConf conf = createJobConf(fs);
      JobClient jc = new JobClient(conf);
      assertEquals(null, conf.getUser());
      RunningJob rj = jc.submitJob(conf);
      assertEquals(ugi.getUserName(), conf.getUser());
      for(; !rj.isComplete(); Thread.sleep(500));
    }
    finally {
      FileUtil.fullyDelete(fs, TEST_ROOT_DIR);
    }
  }
}