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

package org.apache.hadoop.fs.azure.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

/**
 * AzureFileSystemMetricsSystem
 */
@InterfaceAudience.Private
public final class AzureFileSystemMetricsSystem {
  private static MetricsSystemImpl instance;
  private static int numFileSystems;
  
  //private ctor
  private AzureFileSystemMetricsSystem(){
  
  }
  
  public static synchronized void fileSystemStarted() {
    if (numFileSystems == 0) {
      instance = new MetricsSystemImpl();
      instance.init("azure-file-system");
    }
    numFileSystems++;
  }
  
  public static synchronized void fileSystemClosed() {
    if (numFileSystems == 1) {
      instance.publishMetricsNow();
      instance.stop();
      instance.shutdown();
      instance = null;
    }
    numFileSystems--;
  }

  public static void registerSource(String name, String desc,
      MetricsSource source) {
    //caller has to use unique name to register source
    instance.register(name, desc, source);
  }

  public static synchronized void unregisterSource(String name) {
    if (instance != null) {
      //publish metrics before unregister a metrics source
      instance.publishMetricsNow();
      instance.unregisterSource(name);
    }
  }
}
