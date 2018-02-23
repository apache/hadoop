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

package org.apache.hadoop.yarn.applications.unmanagedamlauncher;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The UnmanagedLauncher is a simple client that launches and unmanaged AM. An
 * unmanagedAM is an AM that is not launched and managed by the RM. The client
 * creates a new application on the RM and negotiates a new attempt id. Then it
 * waits for the RM app state to reach be YarnApplicationState.ACCEPTED after
 * which it spawns the AM in another process and passes it the container id via
 * env variable Environment.CONTAINER_ID. The AM can be in any
 * language. The AM can register with the RM using the attempt id obtained
 * from the container id and proceed as normal.
 * The client redirects app stdout and stderr to its own stdout and
 * stderr and waits for the AM process to exit. Then it waits for the RM to
 * report app completion.
 */
public class UnmanagedAMLauncher {
  private static final Logger LOG = LoggerFactory
      .getLogger(UnmanagedAMLauncher.class);

  private Configuration conf;

  // Handle to talk to the Resource Manager/Applications Manager
  protected YarnClient rmClient;

  // Application master specific info to register a new Application with RM/ASM
  private String appName = "";
  // App master priority
  private int amPriority = 0;
  // Queue for App master
  private String amQueue = "";
  // cmd to start AM
  private String amCmd = null;
  // set the classpath explicitly
  private String classpath = null;

  private volatile boolean amCompleted = false;

  private static final long AM_STATE_WAIT_TIMEOUT_MS = 10000;
  /**
   * @param args
   *          Command line arguments
   */
  public static void main(String[] args) {
    try {
      UnmanagedAMLauncher client = new UnmanagedAMLauncher();
      LOG.info("Initializing Client");
      boolean doRun = client.init(args);
      if (!doRun) {
        System.exit(0);
      }
      client.run();
    } catch (Throwable t) {
      LOG.error("Error running Client", t);
      System.exit(1);
    }
  }

  /**
   */
  public UnmanagedAMLauncher(Configuration conf) throws Exception {
    // Set up RPC
    this.conf = conf;
  }

  public UnmanagedAMLauncher() throws Exception {
    this(new Configuration());
  }

  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("Client", opts);
  }

  public boolean init(String[] args) throws ParseException {

    Options opts = new Options();
    opts.addOption("appname", true,
        "Application Name. Default value - UnmanagedAM");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("queue", true,
        "RM Queue in which this application is to be submitted");
    opts.addOption("master_memory", true,
        "Amount of memory in MB to be requested to run the application master");
    opts.addOption("cmd", true, "command to start unmanaged AM (required)");
    opts.addOption("classpath", true, "additional classpath");
    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    appName = cliParser.getOptionValue("appname", "UnmanagedAM");
    amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
    amQueue = cliParser.getOptionValue("queue", "default");
    classpath = cliParser.getOptionValue("classpath", null);

    amCmd = cliParser.getOptionValue("cmd");
    if (amCmd == null) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No cmd specified for application master");
    }

    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    rmClient = YarnClient.createYarnClient();
    rmClient.init(yarnConf);

    return true;
  }

  public void launchAM(ApplicationAttemptId attemptId) 
    throws IOException, YarnException {
    Credentials credentials = new Credentials();
    Token<AMRMTokenIdentifier> token = 
        rmClient.getAMRMToken(attemptId.getApplicationId());
    // Service will be empty but that's okay, we are just passing down only
    // AMRMToken down to the real AM which eventually sets the correct
    // service-address.
    credentials.addToken(token.getService(), token);
    File tokenFile = File.createTempFile("unmanagedAMRMToken","", 
        new File(System.getProperty("user.dir")));
    try {
      FileUtil.chmod(tokenFile.getAbsolutePath(), "600");
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
    tokenFile.deleteOnExit();
    try (DataOutputStream os = new DataOutputStream(
        new FileOutputStream(tokenFile, true))) {
      credentials.writeTokenStorageToStream(os);
    }

    Map<String, String> env = System.getenv();
    ArrayList<String> envAMList = new ArrayList<String>();
    boolean setClasspath = false;
    for (Map.Entry<String, String> entry : env.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if(key.equals("CLASSPATH")) {
        setClasspath = true;
        if(classpath != null) {
          value = value + File.pathSeparator + classpath;
        }
      }
      envAMList.add(key + "=" + value);
    }
    
    if(!setClasspath && classpath!=null) {
      envAMList.add("CLASSPATH="+classpath);
    }
    ContainerId containerId = ContainerId.newContainerId(attemptId, 0);

    String hostname = InetAddress.getLocalHost().getHostName();
    envAMList.add(Environment.CONTAINER_ID.name() + "=" + containerId);
    envAMList.add(Environment.NM_HOST.name() + "=" + hostname);
    envAMList.add(Environment.NM_HTTP_PORT.name() + "=0");
    envAMList.add(Environment.NM_PORT.name() + "=0");
    envAMList.add(Environment.LOCAL_DIRS.name() + "= /tmp");
    envAMList.add(ApplicationConstants.APP_SUBMIT_TIME_ENV + "="
        + System.currentTimeMillis());

    envAMList.add(ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME + "=" + 
      tokenFile.getAbsolutePath());
    
    String[] envAM = new String[envAMList.size()];
    Process amProc = Runtime.getRuntime().exec(amCmd, envAMList.toArray(envAM));

    final BufferedReader errReader = 
        new BufferedReader(new InputStreamReader(
            amProc.getErrorStream(), Charset.forName("UTF-8")));
    final BufferedReader inReader = 
        new BufferedReader(new InputStreamReader(
            amProc.getInputStream(), Charset.forName("UTF-8")));
    
    // read error and input streams as this would free up the buffers
    // free the error stream buffer
    Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            System.err.println(line);
            line = errReader.readLine();
          }
        } catch(IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    Thread outThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = inReader.readLine();
          while((line != null) && !isInterrupted()) {
            System.out.println(line);
            line = inReader.readLine();
          }
        } catch(IOException ioe) {
          LOG.warn("Error reading the out stream", ioe);
        }
      }
    };
    try {
      errThread.start();
      outThread.start();
    } catch (IllegalStateException ise) { }

    // wait for the process to finish and check the exit code
    try {
      int exitCode = amProc.waitFor();
      LOG.info("AM process exited with value: " + exitCode);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      amCompleted = true;
    }
    
    try {
      // make sure that the error thread exits
      // on Windows these threads sometimes get stuck and hang the execution
      // timeout and join later after destroying the process.
      errThread.join();
      outThread.join();
      errReader.close();
      inReader.close();
    } catch (InterruptedException ie) {
      LOG.info("ShellExecutor: Interrupted while reading the error/out stream",
          ie);
    } catch (IOException ioe) {
      LOG.warn("Error while closing the error/out stream", ioe);
    }
    amProc.destroy();
  }
  
  public boolean run() throws IOException, YarnException {
    LOG.info("Starting Client");
    
    // Connect to ResourceManager
    rmClient.start();
    try {  
      // Create launch context for app master
      LOG.info("Setting up application submission context for ASM");
      ApplicationSubmissionContext appContext = rmClient.createApplication()
          .getApplicationSubmissionContext();
      ApplicationId appId = appContext.getApplicationId();

      // set the application name
      appContext.setApplicationName(appName);
  
      // Set the priority for the application master
      Priority pri = Records.newRecord(Priority.class);
      pri.setPriority(amPriority);
      appContext.setPriority(pri);
  
      // Set the queue to which this application is to be submitted in the RM
      appContext.setQueue(amQueue);
  
      // Set up the container launch context for the application master
      ContainerLaunchContext amContainer = Records
          .newRecord(ContainerLaunchContext.class);
      appContext.setAMContainerSpec(amContainer);
  
      // unmanaged AM
      appContext.setUnmanagedAM(true);
      LOG.info("Setting unmanaged AM");
  
      // Submit the application to the applications manager
      LOG.info("Submitting application to ASM");
      rmClient.submitApplication(appContext);

      ApplicationReport appReport =
          monitorApplication(appId, EnumSet.of(YarnApplicationState.ACCEPTED,
            YarnApplicationState.KILLED, YarnApplicationState.FAILED,
            YarnApplicationState.FINISHED));

      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        // Monitor the application attempt to wait for launch state
        ApplicationAttemptReport attemptReport =
            monitorCurrentAppAttempt(appId,
              YarnApplicationAttemptState.LAUNCHED);
        ApplicationAttemptId attemptId =
            attemptReport.getApplicationAttemptId();
        LOG.info("Launching AM with application attempt id " + attemptId);
        // launch AM
        launchAM(attemptId);
        // Monitor the application for end state
        appReport =
            monitorApplication(appId, EnumSet.of(YarnApplicationState.KILLED,
              YarnApplicationState.FAILED, YarnApplicationState.FINISHED));
      }

      YarnApplicationState appState = appReport.getYarnApplicationState();
      FinalApplicationStatus appStatus = appReport.getFinalApplicationStatus();
  
      LOG.info("App ended with state: " + appReport.getYarnApplicationState()
          + " and status: " + appStatus);
      
      boolean success;
      if (YarnApplicationState.FINISHED == appState
          && FinalApplicationStatus.SUCCEEDED == appStatus) {
        LOG.info("Application has completed successfully.");
        success = true;
      } else {
        LOG.info("Application did finished unsuccessfully." + " YarnState="
            + appState.toString() + ", FinalStatus=" + appStatus.toString());
        success = false;
      }
      
      return success;
    } finally {
      rmClient.stop();
    }
  }

  private ApplicationAttemptReport monitorCurrentAppAttempt(
      ApplicationId appId, YarnApplicationAttemptState attemptState)
      throws YarnException, IOException {
    long startTime = System.currentTimeMillis();
    ApplicationAttemptId attemptId = null;
    while (true) {
      if (attemptId == null) {
        attemptId =
            rmClient.getApplicationReport(appId)
              .getCurrentApplicationAttemptId();
      }
      ApplicationAttemptReport attemptReport = null;
      if (attemptId != null) {
        attemptReport = rmClient.getApplicationAttemptReport(attemptId);
        if (attemptState.equals(attemptReport.getYarnApplicationAttemptState())) {
          return attemptReport;
        }
      }
      LOG.info("Current attempt state of " + appId + " is " + (attemptReport == null
            ? " N/A " : attemptReport.getYarnApplicationAttemptState())
                + ", waiting for current attempt to reach " + attemptState);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for current attempt of " + appId
            + " to reach " + attemptState);
      }
      if (System.currentTimeMillis() - startTime > AM_STATE_WAIT_TIMEOUT_MS) {
        String errmsg =
            "Timeout for waiting current attempt of " + appId + " to reach "
                + attemptState;
        LOG.error(errmsg);
        throw new RuntimeException(errmsg);
      }
    }
  }

  /**
   * Monitor the submitted application for completion. Kill application if time
   * expires.
   * 
   * @param appId
   *          Application Id of application to be monitored
   * @return true if application completed successfully
   * @throws YarnException
   * @throws IOException
   */
  private ApplicationReport monitorApplication(ApplicationId appId,
      Set<YarnApplicationState> finalState) throws YarnException,
      IOException {

    long foundAMCompletedTime = 0;
    StringBuilder expectedFinalState = new StringBuilder();
    boolean first = true;
    for (YarnApplicationState state : finalState) {
      if (first) {
        first = false;
        expectedFinalState.append(state.name());
      } else {
        expectedFinalState.append("," + state.name());
      }
    }

    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = rmClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", appAttemptId="
          + report.getCurrentApplicationAttemptId() + ", clientToAMToken="
          + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
          + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
          + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
          + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState="
          + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
          + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        return report;
      }

      // wait for 10 seconds after process has completed for app report to
      // come back
      if (amCompleted) {
        if (foundAMCompletedTime == 0) {
          foundAMCompletedTime = System.currentTimeMillis();
        } else if ((System.currentTimeMillis() - foundAMCompletedTime)
            > AM_STATE_WAIT_TIMEOUT_MS) {
          LOG.warn("Waited " + AM_STATE_WAIT_TIMEOUT_MS/1000
              + " seconds after process completed for AppReport"
              + " to reach desired final state. Not waiting anymore."
              + "CurrentState = " + state
              + ", ExpectedStates = " + expectedFinalState.toString());
          throw new RuntimeException("Failed to receive final expected state"
              + " in ApplicationReport"
              + ", CurrentState=" + state
              + ", ExpectedStates=" + expectedFinalState.toString());
        }
      }
    }
  }
}
