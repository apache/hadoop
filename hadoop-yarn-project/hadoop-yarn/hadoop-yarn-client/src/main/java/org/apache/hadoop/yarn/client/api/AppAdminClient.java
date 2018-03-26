/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.client.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;
import java.util.Map;

/**
 * Client for managing applications.
 */
@Public
@Unstable
public abstract class AppAdminClient extends CompositeService {
  public static final String YARN_APP_ADMIN_CLIENT_PREFIX = "yarn" +
      ".application.admin.client.class.";
  public static final String DEFAULT_TYPE = "yarn-service";
  public static final String DEFAULT_CLASS_NAME = "org.apache.hadoop.yarn" +
      ".service.client.ApiServiceClient";
  public static final String UNIT_TEST_TYPE = "unit-test";
  public static final String UNIT_TEST_CLASS_NAME = "org.apache.hadoop.yarn" +
      ".service.client.ServiceClient";

  @Private
  protected AppAdminClient() {
    super(AppAdminClient.class.getName());
  }

  /**
   * <p>
   * Create a new instance of AppAdminClient.
   * </p>
   *
   * @param appType application type
   * @param conf configuration
   * @return app admin client
   */
  @Public
  @Unstable
  public static AppAdminClient createAppAdminClient(String appType,
      Configuration conf) {
    Map<String, String> clientClassMap =
        conf.getPropsWithPrefix(YARN_APP_ADMIN_CLIENT_PREFIX);
    if (!clientClassMap.containsKey(DEFAULT_TYPE)) {
      clientClassMap.put(DEFAULT_TYPE, DEFAULT_CLASS_NAME);
    }
    if (!clientClassMap.containsKey(UNIT_TEST_TYPE)) {
      clientClassMap.put(UNIT_TEST_TYPE, UNIT_TEST_CLASS_NAME);
    }
    if (!clientClassMap.containsKey(appType)) {
      throw new IllegalArgumentException("App admin client class name not " +
          "specified for type " + appType);
    }
    String clientClassName = clientClassMap.get(appType);
    Class<? extends AppAdminClient> clientClass;
    try {
      clientClass = (Class<? extends AppAdminClient>) Class.forName(
          clientClassName);
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Invalid app admin client class", e);
    }

    AppAdminClient appAdminClient = ReflectionUtils.newInstance(clientClass,
        conf);
    appAdminClient.init(conf);
    appAdminClient.start();
    return appAdminClient;
  }

  /**
   * <p>
   * Launch a new YARN application.
   * </p>
   *
   * @param fileName specification of application
   * @param appName name of the application
   * @param lifetime lifetime of the application
   * @param queue queue of the application
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionLaunch(String fileName, String appName, Long
      lifetime, String queue) throws IOException, YarnException;

  /**
   * <p>
   * Stop a YARN application (attempt to stop gracefully before killing the
   * application). In the case of a long-running service, the service may be
   * restarted later.
   * </p>
   *
   * @param appName the name of the application
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionStop(String appName) throws IOException,
      YarnException;

  /**
   * <p>
   * Start a YARN application from a previously saved specification. In the
   * case of a long-running service, the service must have been previously
   * launched/started and then stopped, or previously saved but not started.
   * </p>
   *
   * @param appName the name of the application
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionStart(String appName) throws IOException,
      YarnException;

  /**
   * <p>
   * Save the specification for a YARN application / long-running service.
   * The application may be started later.
   * </p>
   *
   * @param fileName specification of application to save
   * @param appName name of the application
   * @param lifetime lifetime of the application
   * @param queue queue of the application
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionSave(String fileName, String appName, Long
      lifetime, String queue) throws IOException, YarnException;

  /**
   * <p>
   * Remove the specification and all application data for a YARN application.
   * The application cannot be running.
   * </p>
   *
   * @param appName the name of the application
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionDestroy(String appName) throws IOException,
      YarnException;

  /**
   * <p>
   * Change the number of running containers for a component of a YARN
   * application / long-running service.
   * </p>
   *
   * @param appName the name of the application
   * @param componentCounts map of component name to new component count or
   *                        amount to change existing component count (e.g.
   *                        5, +5, -5)
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionFlex(String appName, Map<String, String>
      componentCounts) throws IOException, YarnException;

  /**
   * <p>
   * Upload AM dependencies to HDFS. This makes future application launches
   * faster since the dependencies do not have to be uploaded on each launch.
   * </p>
   *
   * @param destinationFolder
   *          an optional HDFS folder where dependency tarball will be uploaded
   * @return exit code
   * @throws IOException
   *           IOException
   * @throws YarnException
   *           exception in client or server
   */
  @Public
  @Unstable
  public abstract int enableFastLaunch(String destinationFolder)
      throws IOException, YarnException;

  /**
   * <p>
   * Get detailed app specific status string for a YARN application.
   * </p>
   *
   * @param appIdOrName appId or appName
   * @return status string
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract String getStatusString(String appIdOrName) throws
      IOException, YarnException;

  /**
   * Upgrade a long running service.
   *
   * @param appName the name of the application
   * @param fileName specification of application upgrade to save.
   *
   * @return exit code
   * @throws IOException IOException
   * @throws YarnException exception in client or server
   */
  @Public
  @Unstable
  public abstract int actionUpgrade(String appName, String fileName)
      throws IOException, YarnException;

}
