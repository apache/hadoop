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

import java.io.IOException;

import junit.framework.TestCase;

//import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.util.ServicePlugin;
import org.junit.Test;

public class TestJobTrackerPlugins extends TestCase {
  
  static class FakeServicePlugin implements ServicePlugin {

    private static FakeServicePlugin instance;
    
    public static FakeServicePlugin getInstance() {
      return instance;
    }
    
    private Object service;
    private boolean stopped;
    
    public Object getService() {
      return service;
    }
    
    public boolean isStopped() {
      return stopped;
    }
    
    public FakeServicePlugin() {
      // store static reference to instance so we can retrieve it in the test
      instance = this;
    }
    
    @Override
    public void start(Object service) {
      this.service = service;
    }

    @Override
    public void stop() {
      stopped = true;
    }

    @Override
    public void close() throws IOException {
    }
  }
  
  @Test
  public void test() throws Exception {
    JobConf conf = new JobConf();
    conf.set("mapred.job.tracker", "localhost:0");
    conf.set("mapred.job.tracker.http.address", "0.0.0.0:0");
    conf.setClass("mapreduce.jobtracker.plugins", FakeServicePlugin.class,
        ServicePlugin.class);
    
    assertNull("Plugin not created", FakeServicePlugin.getInstance());
    
    JobTracker jobTracker = JobTracker.startTracker(conf);
    assertNotNull("Plugin created", FakeServicePlugin.getInstance());
    assertSame("Service is jobTracker",
        FakeServicePlugin.getInstance().getService(), jobTracker);
    assertFalse("Plugin not stopped",
        FakeServicePlugin.getInstance().isStopped());
    
    jobTracker.close();
    assertTrue("Plugin stopped", FakeServicePlugin.getInstance().isStopped());
  }

}
