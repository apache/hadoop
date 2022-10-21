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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
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
  private final LinkedList<LogAggregationFileController> controllers = new LinkedList<>();
  private final Configuration conf;

  /**
   * Construct the LogAggregationFileControllerFactory object.
   * @param conf the Configuration
   */
  public LogAggregationFileControllerFactory(Configuration conf) {
    this.conf = conf;
    Collection<String> fileControllers = conf.getStringCollection(
        YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS);
    Map<String, String> controllerChecker = new HashMap<>();

    for (String controllerName : fileControllers) {
      validateAggregatedFileControllerName(controllerName);

      validateConflictingControllers(conf, controllerChecker, controllerName);
      DeterminedControllerClassName className =
          new DeterminedControllerClassName(conf, controllerName);
      LogAggregationFileController controller = createFileControllerInstance(conf,
          controllerName, className);
      controller.initialize(conf, controllerName);
      controllers.add(controller);
    }
  }

  private LogAggregationFileController createFileControllerInstance(
      Configuration conf,
      String fileController, DeterminedControllerClassName className) {
    Class<? extends LogAggregationFileController> clazz = conf.getClass(
        className.configKey, null, LogAggregationFileController.class);
    if (clazz == null) {
      throw new RuntimeException("No class defined for " + fileController);
    }
    LogAggregationFileController instance = ReflectionUtils.newInstance(clazz, conf);
    if (instance == null) {
      throw new RuntimeException("No object created for " + className.value);
    }
    return instance;
  }

  private void validateConflictingControllers(
      Configuration conf, Map<String, String> controllerChecker, String fileController) {
    DeterminedLogAggregationRemoteDir remoteDir =
        new DeterminedLogAggregationRemoteDir(conf, fileController);
    DeterminedLogAggregationSuffix suffix =
        new DeterminedLogAggregationSuffix(conf, fileController);
    String dirSuffix = remoteDir.value + "-" + suffix.value;
    if (controllerChecker.containsKey(dirSuffix)) {
      if (remoteDir.usingDefault && suffix.usingDefault) {
        String fileControllerStr = controllerChecker.get(dirSuffix);
        List<String> controllersList = new ArrayList<>();
        controllersList.add(fileControllerStr);
        controllersList.add(fileController);
        fileControllerStr = StringUtils.join(controllersList, ",");
        controllerChecker.put(dirSuffix, fileControllerStr);
      } else {
        String conflictController = controllerChecker.get(dirSuffix);
        throw new RuntimeException(String.format("The combined value of %s " +
            "and %s should not be the same as the value set for %s",
            remoteDir.configKey, suffix.configKey, conflictController));
      }
    } else {
      controllerChecker.put(dirSuffix, fileController);
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
          diagnosticsMsg.append(ex.getMessage()).append("\n");
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
        diagnosticsMsg.append(ex.getMessage()).append("\n");
      }
    }

    throw new IOException(diagnosticsMsg.toString());
  }

  private void validateAggregatedFileControllerName(String name) {
    boolean valid;
    if (name == null || name.trim().isEmpty()) {
      valid = false;
    } else {
      valid = p.matcher(name).matches();
    }

    Preconditions.checkArgument(valid,
            String.format("The FileControllerName: %s set in " +
                            "%s is invalid.The valid File Controller name should only contain " +
                            "a-zA-Z0-9_ and cannot start with numbers", name,
                    YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS));
  }

  @Private
  @VisibleForTesting
  public LinkedList<LogAggregationFileController>
      getConfiguredLogAggregationFileControllerList() {
    return this.controllers;
  }

  private static class DeterminedLogAggregationRemoteDir {
    private String value;
    private boolean usingDefault = false;
    private final String configKey;

    DeterminedLogAggregationRemoteDir(Configuration conf,
        String fileController) {
      configKey = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
          fileController);
      String remoteDir = conf.get(configKey);

      if (remoteDir == null || remoteDir.isEmpty()) {
        this.value = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR);
        this.usingDefault = true;
      } else {
        this.value = remoteDir;
      }
    }
  }

  private static class DeterminedLogAggregationSuffix {
    private String value;
    private boolean usingDefault = false;
    private final String configKey;

    DeterminedLogAggregationSuffix(Configuration conf,
        String fileController) {
      configKey = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT,
          fileController);
      String suffix = conf.get(configKey);
      if (suffix == null || suffix.isEmpty()) {
        this.value = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
        this.usingDefault = true;
      } else {
        this.value = suffix;
      }
    }
  }

  private static class DeterminedControllerClassName {
    private final String configKey;
    private final String value;

    DeterminedControllerClassName(Configuration conf,
        String fileController) {
      this.configKey = String.format(
          YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT,
          fileController);
      this.value = conf.get(configKey);
      if (value == null || value.isEmpty()) {
        throw new RuntimeException("No class configured for "
            + fileController);
      }
    }
  }
}
