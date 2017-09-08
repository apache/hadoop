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

package org.apache.hadoop.yarn.service.client;

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

import static org.apache.hadoop.yarn.service.client.params.SliderActions.*;

public class ServiceCLI {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceClient.class);
  protected ServiceClient client;

  int exec(ClientArgs args) throws Throwable {
    if (StringUtils.isEmpty(args.getAction())) {
      System.out.println(args.usage());
      return -1;
    }
    switch (args.getAction()) {
    case ACTION_BUILD: // Upload app json onto hdfs
      client.actionBuild(args.getActionBuildArgs());
      break;
    case ACTION_START: // start the app with the pre-uploaded app json on hdfs
      client.actionStart(args.getClusterName());
      break;
    case ACTION_CREATE: // create == build + start
      client.actionCreate(args.getActionCreateArgs());
      break;
    case ACTION_STATUS:
      Service app = client.getStatus(args.getClusterName());
      System.out.println(app);
      break;
    case ACTION_FLEX:
      try {
        client.actionFlexByCLI(args);
      } catch (FileNotFoundException e) {
        System.err.println(
            args.getClusterName() + " doesn't exist: " + e.getMessage());
        return -1;
      }
      break;
    case ACTION_STOP:
      client.actionStop(args.getClusterName(), false);
      break;
    case ACTION_DESTROY: // Destroy can happen only if app is already stopped
      client.actionDestroy(args.getClusterName());
      break;
    case ACTION_DEPENDENCY: // upload dependency jars
      client.actionDependency(args.getActionDependencyArgs());
      break;
    case ACTION_UPDATE:
      client.updateLifetime(args.getClusterName(),
          args.getActionUpdateArgs().lifetime);
      break;
    case ACTION_HELP:
      LOG.info(args.usage());
      break;
    default:
      LOG.info("NOT IMPLEMENTED: " + args.getAction());
      LOG.info(args.usage());
      return -1;
    }
    return 0;
  }

  public ServiceCLI() {
    createServiceClient();
  }

  protected void createServiceClient() {
    client = new ServiceClient();
    client.init(new YarnConfiguration());
    client.start();
  }

  public static void main(String[] args) throws Throwable {
    ClientArgs clientArgs = new ClientArgs(args);
    try {
      clientArgs.parse();
    } catch (ParameterException | SliderException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    }
    ServiceCLI cli =  new ServiceCLI();
    int res = cli.exec(clientArgs);
    System.exit(res);
  }
}
