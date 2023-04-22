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

package org.apache.hadoop.yarn.logaggregation;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT;


public class LogAggregationTestUtils {
  public static final String REMOTE_LOG_ROOT = "target/app-logs/";

  public static void enableFileControllers(Configuration conf,
          List<Class<? extends LogAggregationFileController>> fileControllers,
          List<String> fileControllerNames) {
    enableFcs(conf, REMOTE_LOG_ROOT, fileControllers, fileControllerNames);
  }

  public static void enableFileControllers(Configuration conf,
                                           String remoteLogRoot,
                                           List<Class<? extends LogAggregationFileController>> fileControllers,
                                           List<String> fileControllerNames) {
    enableFcs(conf, remoteLogRoot, fileControllers, fileControllerNames);
  }


  private static void enableFcs(Configuration conf,
                                String remoteLogRoot,
                                List<Class<? extends LogAggregationFileController>> fileControllers,
                                List<String> fileControllerNames) {
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS,
            StringUtils.join(fileControllerNames, ","));
    for (int i = 0; i < fileControllers.size(); i++) {
      Class<? extends LogAggregationFileController> fileController = fileControllers.get(i);
      String controllerName = fileControllerNames.get(i);

      conf.setClass(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, controllerName),
              fileController, LogAggregationFileController.class);
      conf.set(String.format(LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT, controllerName),
              remoteLogRoot + controllerName + "/");
      conf.set(String.format(LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT, controllerName),
              controllerName);
    }
  }
}
