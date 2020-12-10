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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

/**
 * Use {@code LogAggregationFileControllerFactory} to get the correct
 * {@link LogAggregationFileController} for write and read.
 *
 */
@Private
@Unstable
public class LogAggregationFileControllerFactory {

  private static final Logger LOG = LoggerFactory.getLogger(
      LogAggregationFileControllerFactory.class);
  private final Pattern p = Pattern.compile(
      "^[A-Za-z_]+[A-Za-z0-9_]*$");
  private LinkedList<LogAggregationFileController> controllers
      = new LinkedList<>();
  private Configuration conf;

  /**
   * Construct the LogAggregationFileControllerFactory object.
   * @param conf the Configuration
   */
  public LogAggregationFileControllerFactory(Configuration conf) {
    this.conf = conf;
    Collection<String> fileControllers = conf.getStringCollection(
        YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS);
    List<String> controllerClassName = new ArrayList<>();

    Map<String, String> controllerChecker = new HashMap<>();

    for (String fileController : fileControllers) {
      Preconditions.checkArgument(validateAggregatedFileControllerName(
          fileController), "The FileControllerName: " + fileController
          + " set in " + YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS
          +" is invalid." + "The valid File Controller name should only "
          + "contain a-zA-Z0-9_ and can not start with numbers");

      String remoteDirStr = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
          fileController);
      String remoteDir = conf.get(remoteDirStr);
      boolean defaultRemoteDir = false;
      if (remoteDir == null || remoteDir.isEmpty()) {
        remoteDir = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR);
        defaultRemoteDir = true;
      }
      String suffixStr = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT,
          fileController);
      String suffix = conf.get(suffixStr);
      boolean defaultSuffix = false;
      if (suffix == null || suffix.isEmpty()) {
        suffix = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
        defaultSuffix = true;
      }
      String dirSuffix = remoteDir + "-" + suffix;
      if (controllerChecker.containsKey(dirSuffix)) {
        if (defaultRemoteDir && defaultSuffix) {
          String fileControllerStr = controllerChecker.get(dirSuffix);
          List<String> controllersList = new ArrayList<>();
          controllersList.add(fileControllerStr);
          controllersList.add(fileController);
          fileControllerStr = StringUtils.join(controllersList, ",");
          controllerChecker.put(dirSuffix, fileControllerStr);
        } else {
          String conflictController = controllerChecker.get(dirSuffix);
          throw new RuntimeException("The combined value of " + remoteDirStr
              + " and " + suffixStr + " should not be the same as the value"
              + " set for " + conflictController);
        }
      } else {
        controllerChecker.put(dirSuffix, fileController);
      }
      String classKey = String.format(
          YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT,
          fileController);
      String className = conf.get(classKey);
      if (className == null || className.isEmpty()) {
        throw new RuntimeException("No class configured for "
            + fileController);
      }
      controllerClassName.add(className);
      Class<? extends LogAggregationFileController> sClass = conf.getClass(
          classKey, null, LogAggregationFileController.class);
      if (sClass == null) {
        throw new RuntimeException("No class defined for " + fileController);
      }
      LogAggregationFileController s = ReflectionUtils.newInstance(
          sClass, conf);
      if (s == null) {
        throw new RuntimeException("No object created for "
            + controllerClassName);
      }
      s.initialize(conf, fileController);
      controllers.add(s);
    }
  }

  /**
   * Get {@link LogAggregationFileController} to write.
   * @return the LogAggregationFileController instance
   */
  public LogAggregationFileController getFileControllerForWrite() {
    return controllers.getFirst();
  }

  /**
   * Get {@link LogAggregationFileController} to read the aggregated logs
   * for this application.
   * @param appId the ApplicationId
   * @param appOwner the Application Owner
   * @return the LogAggregationFileController instance
   * @throws IOException if can not find any log aggregation file controller
   */
  public LogAggregationFileController getFileControllerForRead(
      ApplicationId appId, String appOwner) throws IOException {
    StringBuilder diagnosticsMsg = new StringBuilder();

    if (LogAggregationUtils.isOlderPathEnabled(conf)) {
      for (LogAggregationFileController fileController : controllers) {
        try {
          Path remoteAppLogDir = fileController.getOlderRemoteAppLogDir(appId,
              appOwner);
          if (LogAggregationUtils.getNodeFiles(conf, remoteAppLogDir, appId,
              appOwner).hasNext()) {
            return fileController;
          }
        } catch (Exception ex) {
          diagnosticsMsg.append(ex.getMessage() + "\n");
          continue;
        }
      }
    }

    for (LogAggregationFileController fileController : controllers) {
      try {
        Path remoteAppLogDir = fileController.getRemoteAppLogDir(
            appId, appOwner);
        if (LogAggregationUtils.getNodeFiles(conf, remoteAppLogDir,
            appId, appOwner).hasNext()) {
          return fileController;
        }
      } catch (Exception ex) {
        diagnosticsMsg.append(ex.getMessage() + "\n");
        continue;
      }
    }

    throw new IOException(diagnosticsMsg.toString());
  }

  private boolean validateAggregatedFileControllerName(String name) {
    if (name == null || name.trim().isEmpty()) {
      return false;
    }
    return p.matcher(name).matches();
  }

  @Private
  @VisibleForTesting
  public LinkedList<LogAggregationFileController>
      getConfiguredLogAggregationFileControllerList() {
    return this.controllers;
  }
}
