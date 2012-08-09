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
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;

public class StoreFactory {
  
  public static Store getStore(Configuration conf) {
    Store store = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.RM_STORE, 
            MemStore.class, Store.class), 
            conf);
    return store;
  }
  
  public static ApplicationStore createVoidAppStore() {
    return new VoidApplicationStore();
  }
  
  private static class VoidApplicationStore implements ApplicationStore {

    public VoidApplicationStore() {}
    
    @Override
    public void storeContainer(Container container) throws IOException {
    }

    @Override
    public void removeContainer(Container container) throws IOException {
    }

    @Override
    public void storeMasterContainer(Container container) throws IOException {
    }

    @Override
    public void updateApplicationState(ApplicationMaster master)
        throws IOException {
    }

    @Override
    public boolean isLoggable() {
      return false;
    }
  }
}