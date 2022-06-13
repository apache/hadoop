package org.apache.hadoop.yarn.logaggregation;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;

import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;

public class LogAggregationTestUtils {
  public static final String REMOTE_LOG_ROOT = "target/app-logs/";
  
  public static void enableFileControllers(Configuration conf,
          List<Class<? extends LogAggregationFileController>> fileControllers,
          List<String> fileControllerNames) {
    enableFileControllersInternal(conf, REMOTE_LOG_ROOT, fileControllers, fileControllerNames);
  }

  public static void enableFileControllers(Configuration conf,
                                           String remoteLogRoot,
                                           List<Class<? extends LogAggregationFileController>> fileControllers,
                                           List<String> fileControllerNames) {
    enableFileControllersInternal(conf, remoteLogRoot, fileControllers, fileControllerNames);
  }


  private static void enableFileControllersInternal(Configuration conf,
                                                    String remoteLogRoot,
                                                    List<Class<? extends LogAggregationFileController>> fileControllers, List<String> fileControllerNames) {
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
