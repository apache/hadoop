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

package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceChildJVM2;
import org.apache.hadoop.mapred.ShuffleHandler;
import org.apache.hadoop.mapred.WrappedJvmID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app2.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class AMContainerHelpers {

  private static final Log LOG = LogFactory.getLog(AMContainerHelpers.class);

  private static Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static final Object classpathLock = new Object();
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;

  /**
   * Create a {@link LocalResource} record with all the given parameters.
   */
  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility)
      throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat
        .getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return BuilderUtils.newLocalResource(resourceURL, type, visibility,
        resourceSize, resourceModificationTime);
  }

  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct a parent CLC and
   * use it for all the containers, so this should go away once the
   * mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf)
      throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      MRApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

  /**
   * Create the common {@link ContainerLaunchContext} for all attempts.
   * 
   * @param applicationACLs
   */
  private static ContainerLaunchContext createCommonContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      Token<JobTokenIdentifier> jobToken,
      final org.apache.hadoop.mapred.JobID oldJobId, Credentials credentials) {

    // Application resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[] {});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // //////////// Set up JobJar to be localized properly on the remote NM.
      String jobJar = conf.get(MRJobConfig.JAR);
      if (jobJar != null) {
        Path remoteJobJar = (new Path(jobJar)).makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory());
        localResources.put(
            MRJobConfig.JOB_JAR,
            createLocalResource(remoteFS, remoteJobJar, LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION));
        LOG.info("The job-jar file on the remote FS is "
            + remoteJobJar.toUri().toASCIIString());
      } else {
        // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
        // mapreduce jar itself which is already on the classpath.
        LOG.info("Job jar is not present. "
            + "Not adding any jar to the list of resources.");
      }
      // //////////// End of JobJar setup

      // //////////// Set up JobConf to be localized properly on the remote NM.
      Path path = MRApps.getStagingAreaDir(conf, UserGroupInformation
          .getCurrentUser().getShortUserName());
      Path remoteJobSubmitDir = new Path(path, oldJobId.toString());
      Path remoteJobConfPath = new Path(remoteJobSubmitDir,
          MRJobConfig.JOB_CONF_FILE);
      localResources.put(
          MRJobConfig.JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath,
              LocalResourceType.FILE, LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is "
          + remoteJobConfPath.toUri().toASCIIString());
      // //////////// End of JobConf setup

      // Setup DistributedCache
      MRApps.setupDistributedCache(conf, localResources);

      // Setup up task credentials buffer
      Credentials taskCredentials = new Credentials();

      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Adding #" + credentials.numberOfTokens() + " tokens and #"
            + credentials.numberOfSecretKeys()
            + " secret keys for NM use for launching container");
        taskCredentials.addAll(credentials);
      }

      // LocalStorageToken is needed irrespective of whether security is enabled
      // or not.
      TokenCache.setJobToken(jobToken, taskCredentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is "
          + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      taskCredentialsBuffer = ByteBuffer.wrap(containerTokens_dob.getData(), 0,
          containerTokens_dob.getLength());

      // Add shuffle token
      LOG.info("Putting shuffle token in serviceData");
      serviceData.put(ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID,
          ShuffleHandler.serializeServiceData(jobToken));

      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(),
          getInitialClasspath(conf));
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Shell
    environment.put(Environment.SHELL.name(), conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_SHELL, MRJobConfig.DEFAULT_SHELL));

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(),
        Environment.PWD.$());

    // Add the env variables passed by the admin
    Apps.setEnvFromInputString(environment, conf.get(
        MRJobConfig.MAPRED_ADMIN_USER_ENV,
        MRJobConfig.DEFAULT_MAPRED_ADMIN_USER_ENV));

    // Construct the actual Container
    // The null fields are per-container and will be constructed for each
    // container separately.
    ContainerLaunchContext container = BuilderUtils.newContainerLaunchContext(
        null, conf.get(MRJobConfig.USER_NAME), null, localResources,
        environment, null, serviceData, taskCredentialsBuffer, applicationACLs);

    return container;
  }

  @VisibleForTesting
  public static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs,
      ContainerId containerID, JobConf jobConf, TaskType taskType,
      Token<JobTokenIdentifier> jobToken,
      final org.apache.hadoop.mapred.JobID oldJobId,
      Resource assignedCapability, WrappedJvmID jvmID,
      TaskAttemptListener taskAttemptListener, Credentials credentials,
      boolean shouldProfile) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec = createCommonContainerLaunchContext(
            applicationACLs, jobConf, jobToken, oldJobId, credentials);
      }
    }

    // Fill in the fields needed per-container that are missing in the common
    // spec.

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);
    MapReduceChildJVM2.setVMEnv(myEnv, jobConf, taskType);

    // Set up the launch command
    List<String> commands = MapReduceChildJVM2.getVMCommand(
        taskAttemptListener.getAddress(), jobConf, taskType, jvmID,
        oldJobId, shouldProfile);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData()
        .entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container = BuilderUtils.newContainerLaunchContext(
        containerID, commonContainerSpec.getUser(), assignedCapability,
        commonContainerSpec.getLocalResources(), myEnv, commands,
        myServiceData, commonContainerSpec.getContainerTokens().duplicate(),
        applicationACLs);

    return container;
  }

}
