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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
/**
 * A simple class for storing RM state in any storage that implements a basic
 * FileSystem interface. Does not use directories so that simple key-value
 * stores can be used. The retry policy for the real filesystem client must be
 * configured separately to enable retry of filesystem operations when needed.
 */
public class FileSystemRMStateStore extends RMStateStore {

  public static final Log LOG = LogFactory.getLog(FileSystemRMStateStore.class);

  private static final String ROOT_DIR_NAME = "FSRMStateRoot";


  private FileSystem fs;

  private Path fsRootDirPath;

  @VisibleForTesting
  Path fsWorkingPath;

  public synchronized void initInternal(Configuration conf)
      throws Exception{

    fsWorkingPath = new Path(conf.get(YarnConfiguration.FS_RM_STATE_STORE_URI));
    fsRootDirPath = new Path(fsWorkingPath, ROOT_DIR_NAME);

    // create filesystem
    fs = fsWorkingPath.getFileSystem(conf);
    fs.mkdirs(fsRootDirPath);
  }

  @Override
  protected synchronized void closeInternal() throws Exception {
    fs.close();
  }

  @Override
  public synchronized RMState loadState() throws Exception {
    try {
      RMState state = new RMState();
      FileStatus[] childNodes = fs.listStatus(fsRootDirPath);
      List<ApplicationAttemptState> attempts =
                                      new ArrayList<ApplicationAttemptState>();
      for(FileStatus childNodeStatus : childNodes) {
        assert childNodeStatus.isFile();
        String childNodeName = childNodeStatus.getPath().getName();
        Path childNodePath = getNodePath(childNodeName);
        byte[] childData = readFile(childNodePath, childNodeStatus.getLen());
        if(childNodeName.startsWith(ApplicationId.appIdStrPrefix)){
          // application
          LOG.info("Loading application from node: " + childNodeName);
          ApplicationId appId = ConverterUtils.toApplicationId(childNodeName);
          ApplicationStateDataPBImpl appStateData =
              new ApplicationStateDataPBImpl(
                                ApplicationStateDataProto.parseFrom(childData));
          ApplicationState appState = new ApplicationState(
                               appStateData.getSubmitTime(),
                               appStateData.getApplicationSubmissionContext());
          // assert child node name is same as actual applicationId
          assert appId.equals(appState.context.getApplicationId());
          state.appState.put(appId, appState);
        } else if(childNodeName.startsWith(
                                ApplicationAttemptId.appAttemptIdStrPrefix)) {
          // attempt
          LOG.info("Loading application attempt from node: " + childNodeName);
          ApplicationAttemptId attemptId =
                          ConverterUtils.toApplicationAttemptId(childNodeName);
          ApplicationAttemptStateDataPBImpl attemptStateData =
              new ApplicationAttemptStateDataPBImpl(
                  ApplicationAttemptStateDataProto.parseFrom(childData));
          ApplicationAttemptState attemptState = new ApplicationAttemptState(
                          attemptId, attemptStateData.getMasterContainer());
          // assert child node name is same as application attempt id
          assert attemptId.equals(attemptState.getAttemptId());
          attempts.add(attemptState);
        } else {
          LOG.info("Unknown child node with name: " + childNodeName);
        }
      }

      // go through all attempts and add them to their apps
      for(ApplicationAttemptState attemptState : attempts) {
        ApplicationId appId = attemptState.getAttemptId().getApplicationId();
        ApplicationState appState = state.appState.get(appId);
        if(appState != null) {
          appState.attempts.put(attemptState.getAttemptId(), attemptState);
        } else {
          // the application node may have been removed when the application
          // completed but the RM might have stopped before it could remove the
          // application attempt nodes
          LOG.info("Application node not found for attempt: "
                    + attemptState.getAttemptId());
          deleteFile(getNodePath(attemptState.getAttemptId().toString()));
        }
      }

      return state;
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }

  @Override
  public synchronized void storeApplicationState(String appId,
                                     ApplicationStateDataPBImpl appStateDataPB)
                                     throws Exception {
    Path nodeCreatePath = getNodePath(appId);

    LOG.info("Storing info for app: " + appId + " at: " + nodeCreatePath);
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      writeFile(nodeCreatePath, appStateData);
    } catch (Exception e) {
      LOG.info("Error storing info for app: " + appId, e);
      throw e;
    }
  }

  @Override
  public synchronized void storeApplicationAttemptState(String attemptId,
                          ApplicationAttemptStateDataPBImpl attemptStateDataPB)
                          throws Exception {
    Path nodeCreatePath = getNodePath(attemptId);
    LOG.info("Storing info for attempt: " + attemptId
             + " at: " + nodeCreatePath);
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA
      // based on whether we have lost the right to write to FS
      writeFile(nodeCreatePath, attemptStateData);
    } catch (Exception e) {
      LOG.info("Error storing info for attempt: " + attemptId, e);
      throw e;
    }
  }

  @Override
  public synchronized void removeApplicationState(ApplicationState appState)
                                                            throws Exception {
    String appId = appState.getAppId().toString();
    Path nodeRemovePath = getNodePath(appId);
    LOG.info("Removing info for app: " + appId + " at: " + nodeRemovePath);
    deleteFile(nodeRemovePath);
    for(ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      removeApplicationAttemptState(attemptId.toString());
    }
  }

  public synchronized void removeApplicationAttemptState(String attemptId)
                                                            throws Exception {
    Path nodeRemovePath = getNodePath(attemptId);
    LOG.info("Removing info for attempt: " + attemptId
             + " at: " + nodeRemovePath);
    deleteFile(nodeRemovePath);
  }

  // FileSystem related code

  private void deleteFile(Path deletePath) throws Exception {
    if(!fs.delete(deletePath, true)) {
      throw new Exception("Failed to delete " + deletePath);
    }
  }

  private byte[] readFile(Path inputPath, long len) throws Exception {
    FSDataInputStream fsIn = fs.open(inputPath);
    // state data will not be that "long"
    byte[] data = new byte[(int)len];
    fsIn.readFully(data);
    return data;
  }

  private void writeFile(Path outputPath, byte[] data) throws Exception {
    FSDataOutputStream fsOut = fs.create(outputPath, false);
    fsOut.write(data);
    fsOut.flush();
    fsOut.close();
  }

  @VisibleForTesting
  Path getNodePath(String nodeName) {
    return new Path(fsRootDirPath, nodeName);
  }
}
