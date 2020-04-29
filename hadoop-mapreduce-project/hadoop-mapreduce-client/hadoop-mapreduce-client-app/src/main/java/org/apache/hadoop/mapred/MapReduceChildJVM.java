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

package org.apache.hadoop.mapred;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@SuppressWarnings("deprecation")
public class MapReduceChildJVM {

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + 
        filter.toString();
  }

  private static String getChildEnvProp(JobConf jobConf, boolean isMap) {
    if (isMap) {
      return JobConf.MAPRED_MAP_TASK_ENV;
    }
    return JobConf.MAPRED_REDUCE_TASK_ENV;
  }

  private static String getChildEnvDefaultValue(JobConf jobConf) {
    // There is no default value for these - use the fallback value instead.
    return jobConf.get(JobConf.MAPRED_TASK_ENV);
  }

  public static void setVMEnv(Map<String, String> environment,
      Task task) {

    JobConf conf = task.conf;
    boolean isMap = task.isMapTask();

    // Remove these before adding the user variables to prevent
    // MRApps.setEnvFromInputProperty() from appending to them.
    String hadoopRootLoggerKey = "HADOOP_ROOT_LOGGER";
    String hadoopClientOptsKey = "HADOOP_CLIENT_OPTS";
    environment.remove(hadoopRootLoggerKey);
    environment.remove(hadoopClientOptsKey);

    // Add the environment variables passed by the user
    MRApps.setEnvFromInputProperty(environment, getChildEnvProp(conf, isMap),
        getChildEnvDefaultValue(conf), conf);

    // Set HADOOP_ROOT_LOGGER and HADOOP_CLIENTS if the user did not set them.
    if (!environment.containsKey(hadoopRootLoggerKey)) {
      // Set the value for logging level in the environment.
      // This is so that, if the child forks another "bin/hadoop" (common in
      // streaming) it will have the correct loglevel.
      environment.put(hadoopRootLoggerKey,
          MRApps.getChildLogLevel(conf, task.isMapTask()) + ",console");
    }
    if (!environment.containsKey(hadoopClientOptsKey)) {
      // TODO: The following is useful for instance in streaming tasks.
      // Should be set in ApplicationMaster's env by the RM.
      String hadoopClientOptsValue = System.getenv(hadoopClientOptsKey);
      if (hadoopClientOptsValue == null) {
        hadoopClientOptsValue = "";
      } else {
        hadoopClientOptsValue = hadoopClientOptsValue + " ";
      }
      environment.put(hadoopClientOptsKey, hadoopClientOptsValue);
    }

    // Add stdout/stderr env
    environment.put(
        MRJobConfig.STDOUT_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDOUT)
        );
    environment.put(
        MRJobConfig.STDERR_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDERR)
        );
  }

  private static String getChildJavaOpts(JobConf jobConf, boolean isMapTask) {
    return jobConf.getTaskJavaOpts(isMapTask ? TaskType.MAP : TaskType.REDUCE);
  }

  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, Task task, 
      JVMId jvmID) {

    TaskAttemptID attemptID = task.getTaskID();
    JobConf conf = task.conf;

    Vector<String> vargs = new Vector<String>(8);

    vargs.add(MRApps.crossPlatformifyMREnv(task.conf, Environment.JAVA_HOME)
        + "/bin/java");

    // Add child (task) java-vm options.
    //
    // The following symbols if present in mapred.{map|reduce}.child.java.opts 
    // value are replaced:
    // + @taskid@ is interpolated with value of TaskID.
    // Other occurrences of @ will not be altered.
    //
    // Example with multiple arguments and substitutions, showing
    // jvm GC logging, and start of a passwordless JVM JMX agent so can
    // connect with jconsole and the likes to watch child memory, threads
    // and get thread dumps.
    //
    //  <property>
    //    <name>mapred.map.child.java.opts</name>
    //    <value>-Xmx 512M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
    //           -Dcom.sun.management.jmxremote.authenticate=false \
    //           -Dcom.sun.management.jmxremote.ssl=false \
    //    </value>
    //  </property>
    //
    //  <property>
    //    <name>mapred.reduce.child.java.opts</name>
    //    <value>-Xmx 1024M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
    //           -Dcom.sun.management.jmxremote.authenticate=false \
    //           -Dcom.sun.management.jmxremote.ssl=false \
    //    </value>
    //  </property>
    //
    String javaOpts = getChildJavaOpts(conf, task.isMapTask());
    javaOpts = javaOpts.replace("@taskid@", attemptID.toString());
    String [] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    Path childTmpDir = new Path(MRApps.crossPlatformifyMREnv(conf, Environment.PWD),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    MRApps.addLog4jSystemProperties(task, vargs, conf);

    if (conf.getProfileEnabled()) {
      if (conf.getProfileTaskRange(task.isMapTask()
                                   ).isIncluded(task.getPartition())) {
        final String profileParams = conf.get(task.isMapTask()
            ? MRJobConfig.TASK_MAP_PROFILE_PARAMS
            : MRJobConfig.TASK_REDUCE_PROFILE_PARAMS, conf.getProfileParams());
        vargs.add(String.format(profileParams,
            getTaskLogFile(TaskLog.LogName.PROFILE)));
      }
    }

    // Add main class and its arguments 
    vargs.add(YarnChild.class.getName());  // main of Child
    // pass TaskAttemptListener's address
    vargs.add(taskAttemptListenerAddr.getAddress().getHostAddress()); 
    vargs.add(Integer.toString(taskAttemptListenerAddr.getPort())); 
    vargs.add(attemptID.toString());                      // pass task identifier

    // Finally add the jvmID
    vargs.add(String.valueOf(jvmID.getId()));
    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }
}
