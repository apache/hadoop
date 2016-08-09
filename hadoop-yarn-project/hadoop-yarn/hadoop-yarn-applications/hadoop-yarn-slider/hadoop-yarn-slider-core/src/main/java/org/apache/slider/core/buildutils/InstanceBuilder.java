/*
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

package org.apache.slider.core.buildutils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.persist.ConfPersister;
import org.apache.slider.core.persist.InstancePaths;
import org.apache.slider.core.persist.LockAcquireFailedException;
import org.apache.slider.core.persist.LockHeldAction;
import org.apache.slider.core.zk.ZKPathBuilder;
import org.apache.slider.core.zk.ZookeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.slider.api.InternalKeys.INTERNAL_ADDONS_DIR_PATH;
import static org.apache.slider.api.InternalKeys.INTERNAL_APPDEF_DIR_PATH;
import static org.apache.slider.api.InternalKeys.INTERNAL_QUEUE;
import static org.apache.slider.api.OptionKeys.INTERNAL_AM_TMP_DIR;
import static org.apache.slider.api.OptionKeys.INTERNAL_TMP_DIR;
import static org.apache.slider.api.OptionKeys.INTERNAL_APPLICATION_HOME;
import static org.apache.slider.api.OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH;
import static org.apache.slider.api.OptionKeys.INTERNAL_DATA_DIR_PATH;
import static org.apache.slider.api.OptionKeys.INTERNAL_GENERATED_CONF_PATH;
import static org.apache.slider.api.OptionKeys.INTERNAL_SNAPSHOT_CONF_PATH;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_HOSTS;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_PATH;
import static org.apache.slider.api.OptionKeys.ZOOKEEPER_QUORUM;

/**
 * Build up the instance of a cluster.
 */
public class InstanceBuilder {

  private final String clustername;
  private final Configuration conf;
  private final CoreFileSystem coreFS;
  private final InstancePaths instancePaths;
  private AggregateConf instanceDescription;

  private static final Logger log =
    LoggerFactory.getLogger(InstanceBuilder.class);

  public InstanceBuilder(CoreFileSystem coreFileSystem,
                         Configuration conf,
                         String clustername) {
    this.clustername = clustername;
    this.conf = conf;
    this.coreFS = coreFileSystem;
    Path instanceDir = coreFileSystem.buildClusterDirPath(clustername);
    instancePaths = new InstancePaths(instanceDir);

  }

  public AggregateConf getInstanceDescription() {
    return instanceDescription;
  }

  public InstancePaths getInstancePaths() {
    return instancePaths;
  }


  @Override
  public String toString() {
    return "Builder working with " + clustername + " at " +
           getInstanceDir();
  }

  private Path getInstanceDir() {
    return instancePaths.instanceDir;
  }

  /**
   * Initial part of the build process
   * @param instanceConf
   * @param provider
   */
  public void init(
    String provider,
    AggregateConf instanceConf) {


    this.instanceDescription = instanceConf;

    //internal is extended
    ConfTreeOperations internalOps = instanceConf.getInternalOperations();

    Map<String, Object> md = internalOps.getConfTree().metadata;
    long time = System.currentTimeMillis();
    md.put(StatusKeys.INFO_CREATE_TIME_HUMAN, SliderUtils.toGMTString(time));
    md.put(StatusKeys.INFO_CREATE_TIME_MILLIS, Long.toString(time));

    MapOperations globalOptions = internalOps.getGlobalOptions();
    BuildHelper.addBuildMetadata(md, "create");
    SliderUtils.setInfoTime(md,
        StatusKeys.INFO_CREATE_TIME_HUMAN,
        StatusKeys.INFO_CREATE_TIME_MILLIS,
        System.currentTimeMillis());

    internalOps.set(INTERNAL_AM_TMP_DIR,
                    instancePaths.tmpPathAM.toUri());
    internalOps.set(INTERNAL_TMP_DIR,
                    instancePaths.tmpPath.toUri());
    internalOps.set(INTERNAL_SNAPSHOT_CONF_PATH,
                    instancePaths.snapshotConfPath.toUri());
    internalOps.set(INTERNAL_GENERATED_CONF_PATH,
                    instancePaths.generatedConfPath.toUri());
    internalOps.set(INTERNAL_DATA_DIR_PATH,
                    instancePaths.dataPath.toUri());
    internalOps.set(INTERNAL_APPDEF_DIR_PATH,
                    instancePaths.appDefPath.toUri());
    internalOps.set(INTERNAL_ADDONS_DIR_PATH,
                    instancePaths.addonsPath.toUri());


    internalOps.set(InternalKeys.INTERNAL_PROVIDER_NAME, provider);
    internalOps.set(OptionKeys.APPLICATION_NAME, clustername);

  }

  /**
   * Set the queue used to start the application
   * @param queue
   * @throws BadConfigException
   */
  public void setQueue(String queue) throws BadConfigException {
    if(queue != null) {
      if(SliderUtils.isUnset(queue)) {
        throw new BadConfigException("Queue value cannot be empty.");
      }

      instanceDescription.getInternalOperations().set(INTERNAL_QUEUE, queue);
    }
  }

  /**
   * Set up the image/app home path
   * @param appImage   path in the DFS to the tar file
   * @param appHomeDir other strategy: home dir
   * @throws BadConfigException if both are found
   */
  public void setImageDetailsIfAvailable(
      Path appImage,
      String appHomeDir) throws BadConfigException {
    boolean appHomeUnset = SliderUtils.isUnset(appHomeDir);
    // App home or image
    if (appImage != null) {
      if (!appHomeUnset) {
        // both args have been set
        throw new BadConfigException(
            ErrorStrings.E_BOTH_IMAGE_AND_HOME_DIR_SPECIFIED);
      }
      instanceDescription.getInternalOperations().set(INTERNAL_APPLICATION_IMAGE_PATH,
                                                      appImage.toUri());
    } else {
      // the alternative is app home, which now MUST be set
      if (!appHomeUnset) {
        instanceDescription.getInternalOperations().set(INTERNAL_APPLICATION_HOME,
                                                        appHomeDir);
      }
    }
  }


  /**
   * Propagate any critical principals from the current site config down to the HBase one.
   */
  public void propagatePrincipals() {
    String dfsPrincipal = conf.get(SliderXmlConfKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    if (dfsPrincipal != null) {
      String siteDfsPrincipal = OptionKeys.SITE_XML_PREFIX +
                                SliderXmlConfKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
      instanceDescription.getAppConfOperations().set(siteDfsPrincipal, dfsPrincipal);
    }
  }

  public void propagateFilename() {
    String fsDefaultName = conf.get(
      CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    instanceDescription.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
                                            CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                                            fsDefaultName
                                           );

    instanceDescription.getAppConfOperations().set(OptionKeys.SITE_XML_PREFIX +
                                            SliderXmlConfKeys.FS_DEFAULT_NAME_CLASSIC,
                                            fsDefaultName
                                           );

  }


  public void takeSnapshotOfConfDir(Path appconfdir) throws
                                                     IOException,
                                                     BadConfigException,
                                                     BadClusterStateException {
    FileSystem srcFS = FileSystem.get(appconfdir.toUri(), conf);
    if (!srcFS.isDirectory(appconfdir)) {
      throw new BadConfigException(
        "Source Configuration directory is not valid: %s",
        appconfdir.toString());
    }
    // bulk copy
    FsPermission clusterPerms = coreFS.getInstanceDirectoryPermissions();
    // first the original from wherever to the DFS
    SliderUtils.copyDirectory(conf, appconfdir, instancePaths.snapshotConfPath,
        clusterPerms);
  }


  /**
   * Persist this
   * @param appconfdir conf dir
   * @param overwrite if true, we don't need to create cluster dir
   * @throws IOException
   * @throws SliderException
   * @throws LockAcquireFailedException
   */
  public void persist(Path appconfdir, boolean overwrite) throws
      IOException,
      SliderException,
      LockAcquireFailedException {
    if (!overwrite) {
      coreFS.createClusterDirectories(instancePaths);
    }
    ConfPersister persister =
      new ConfPersister(coreFS, getInstanceDir());
    ConfDirSnapshotAction action = null;
    if (appconfdir != null) {
      action = new ConfDirSnapshotAction(appconfdir);
    }
    persister.save(instanceDescription, action);
  }

  /**
   * Add the ZK paths to the application options. 
   * 
   * @param zkBinding ZK binding
   */
  public void addZKBinding(ZKPathBuilder zkBinding) throws BadConfigException {

    String quorum = zkBinding.getAppQuorum();
    if (SliderUtils.isSet(quorum)) {
      MapOperations globalAppOptions =
          instanceDescription.getAppConfOperations().getGlobalOptions();
      globalAppOptions.put(ZOOKEEPER_PATH, zkBinding.getAppPath());
      globalAppOptions.put(ZOOKEEPER_QUORUM, quorum);
      globalAppOptions.put(ZOOKEEPER_HOSTS,
          ZookeeperUtils.convertToHostsOnlyList(quorum));
    }
  }

  /**
   * Class to execute the snapshotting of the configuration directory
   * while the persistence lock is held. 
   * 
   * This guarantees that there won't be an attempt to launch a cluster
   * until the snapshot is complete -as the write lock won't be released
   * until afterwards.
   */
  private class ConfDirSnapshotAction implements LockHeldAction {

    private final Path appconfdir;

    private ConfDirSnapshotAction(Path appconfdir) {
      this.appconfdir = appconfdir;
    }

    @Override
    public void execute() throws IOException, SliderException {

      takeSnapshotOfConfDir(appconfdir);
    }
  }
  
}
