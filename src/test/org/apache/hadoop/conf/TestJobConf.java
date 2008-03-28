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

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class TestJobConf extends TestCase {

  public void testProfileParamsDefaults() {
    JobConf configuration = new JobConf();

    Assert.assertNull(configuration.get("mapred.task.profile.params"));

    String result = configuration.getProfileParams();

    Assert.assertNotNull(result);
    Assert.assertTrue(result.contains("file=%s"));
    Assert.assertTrue(result.startsWith("-agentlib:hprof"));
  }

  public void testProfileParamsSetter() {
    JobConf configuration = new JobConf();

    configuration.setProfileParams("test");
    Assert.assertEquals("test", configuration.get("mapred.task.profile.params"));
  }

  public void testProfileParamsGetter() {
    JobConf configuration = new JobConf();

    configuration.set("mapred.task.profile.params", "test");
    Assert.assertEquals("test", configuration.getProfileParams());
  }

  public void testInputPath() throws Exception {
    JobConf jobConf = new JobConf();
    Path path = new Path(jobConf.getWorkingDirectory(), 
        "xx{y"+StringUtils.COMMA_STR+"z}");
    jobConf.setInputPath(path);
    Path[] paths = jobConf.getInputPaths();
    assertEquals(1, paths.length);
    assertEquals(path.toString(), paths[0].toString());
    
    StringBuilder pathStr = new StringBuilder();
    pathStr.append(StringUtils.ESCAPE_CHAR);
    pathStr.append(StringUtils.ESCAPE_CHAR);
    pathStr.append(StringUtils.COMMA);
    pathStr.append(StringUtils.COMMA);
    pathStr.append('a');
    path = new Path(jobConf.getWorkingDirectory(), pathStr.toString());
    jobConf.setInputPath(path);
    paths = jobConf.getInputPaths();
    assertEquals(1, paths.length);
    assertEquals(path.toString(), paths[0].toString());
    
    pathStr.setLength(0);
    pathStr.append(StringUtils.ESCAPE_CHAR);
    pathStr.append("xx");
    pathStr.append(StringUtils.ESCAPE_CHAR);
    path = new Path(jobConf.getWorkingDirectory(), pathStr.toString());
    Path path1 = new Path(jobConf.getWorkingDirectory(),
        "yy"+StringUtils.COMMA_STR+"zz");
    jobConf.setInputPath(path);
    jobConf.addInputPath(path1);
    paths = jobConf.getInputPaths();
    assertEquals(2, paths.length);
    assertEquals(path.toString(), paths[0].toString());
    assertEquals(path1.toString(), paths[1].toString());
  }
}
