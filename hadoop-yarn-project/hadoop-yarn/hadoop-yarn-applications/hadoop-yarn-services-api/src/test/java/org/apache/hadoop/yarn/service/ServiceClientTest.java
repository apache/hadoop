/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;

/**
 * A mock version of ServiceClient - This class is design
 * to simulate various error conditions that will happen
 * when a consumer class calls ServiceClient.
 */
public class ServiceClientTest extends ServiceClient {

  private Configuration conf = new Configuration();

  protected static void init() {
  }

  public ServiceClientTest() {
    super();
  }

  @Override
  public Configuration getConfig() {
    return conf;
  }

  @Override
  public ApplicationId actionCreate(Service service) throws IOException {
    ServiceApiUtil.validateAndResolveService(service,
        new SliderFileSystem(conf), getConfig());
    return ApplicationId.newInstance(System.currentTimeMillis(), 1);
  }

  @Override
  public Service getStatus(String appName) {
    if (appName == null) {
      throw new NullPointerException();
    }
    if (appName.equals("jenkins")) {
      return new Service();
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public int actionStart(String serviceName)
      throws YarnException, IOException {
    if (serviceName == null) {
      throw new NullPointerException();
    }
    if (serviceName.equals("jenkins")) {
      return EXIT_SUCCESS;
    } else {
      throw new ApplicationNotFoundException("");
    }
  }

  @Override
  public int actionStop(String serviceName, boolean waitForAppStopped)
      throws YarnException, IOException {
    if (serviceName == null) {
      throw new NullPointerException();
    }
    if (serviceName.equals("jenkins")) {
      return EXIT_SUCCESS;
    } else if (serviceName.equals("jenkins-second-stop")) {
      return EXIT_COMMAND_ARGUMENT_ERROR;
    } else {
      throw new ApplicationNotFoundException("");
    }
  }

  @Override
  public int actionDestroy(String serviceName) {
    if (serviceName == null) {
      throw new NullPointerException();
    }
    if (serviceName.equals("jenkins")) {
      return EXIT_SUCCESS;
    } else {
      throw new IllegalArgumentException();
    }
  }
}
