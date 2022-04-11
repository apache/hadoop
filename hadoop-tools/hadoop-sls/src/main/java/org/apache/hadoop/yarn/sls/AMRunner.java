/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.sls.SLSRunner.TraceType;
import org.apache.hadoop.yarn.sls.appmaster.AMSimulator;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;
import org.apache.hadoop.yarn.util.UTCClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class AMRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AMRunner.class);
  int remainingApps = 0;

  private final Configuration conf;
  private int AM_ID;
  private Map<String, AMSimulator> amMap;
  private Map<ApplicationId, AMSimulator> appIdAMSim;
  private Set<String> trackedApps;
  private Map<String, Class> amClassMap;
  private TraceType inputType;
  private String[] inputTraces;
  private final TaskRunner runner;
  private final SLSRunner slsRunner;
  private int numAMs, numTasks;
  private long maxRuntime;
  private ResourceManager rm;

  public AMRunner(TaskRunner runner, SLSRunner slsRunner) {
    this.runner = runner;
    this.slsRunner = slsRunner;
    this.conf = slsRunner.getConf();
  }


  public void init(Configuration conf) throws ClassNotFoundException {
    amMap = new ConcurrentHashMap<>();
    amClassMap = new HashMap<>();
    appIdAMSim = new ConcurrentHashMap<>();
    // <AMType, Class> map
    for (Map.Entry<String, String> e : conf) {
      String key = e.getKey();
      if (key.startsWith(SLSConfiguration.AM_TYPE_PREFIX)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE_PREFIX.length());
        amClassMap.put(amType, Class.forName(conf.get(key)));
      }
    }
  }

  public void startAM() throws YarnException, IOException {
    switch (inputType) {
      case SLS:
        for (String inputTrace : inputTraces) {
          startAMFromSLSTrace(inputTrace);
        }
        break;
      case RUMEN:
        long baselineTimeMS = 0;
        for (String inputTrace : inputTraces) {
          startAMFromRumenTrace(inputTrace, baselineTimeMS);
        }
        break;
      case SYNTH:
        startAMFromSynthGenerator();
        break;
      default:
        throw new YarnException("Input configuration not recognized, "
            + "trace type should be SLS, RUMEN, or SYNTH");
    }

    numAMs = amMap.size();
    remainingApps = numAMs;
  }

  /**
   * Parse workload from a SLS trace file.
   */
  private void startAMFromSLSTrace(String inputTrace) throws IOException {
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();

    try (Reader input = new InputStreamReader(
        new FileInputStream(inputTrace), StandardCharsets.UTF_8)) {
      JavaType type = mapper.getTypeFactory().
          constructMapType(Map.class, String.class, String.class);
      Iterator<Map<String, String>> jobIter = mapper.readValues(
          jsonF.createParser(input), type);

      while (jobIter.hasNext()) {
        try {
          Map<String, String> jsonJob = jobIter.next();
          AMDefinitionSLS amDef = AMDefinitionFactory.createFromSlsTrace(jsonJob, slsRunner);
          startAMs(amDef);
        } catch (Exception e) {
          LOG.error("Failed to create an AM: {}", e.getMessage());
        }
      }
    }
  }

  /**
   * parse workload information from synth-generator trace files.
   */
  private void startAMFromSynthGenerator() throws YarnException, IOException {
    Configuration localConf = new Configuration();
    localConf.set("fs.defaultFS", "file:///");
    //if we use the nodeFile this could have been not initialized yet.
    if (slsRunner.getStjp() == null) {
      slsRunner.setStjp(new SynthTraceJobProducer(conf, new Path(inputTraces[0])));
    }

    SynthJob job;
    // we use stjp, a reference to the job producer instantiated during node
    // creation
    while ((job = (SynthJob) slsRunner.getStjp().getNextJob()) != null) {
      ReservationId reservationId = null;
      if (job.hasDeadline()) {
        reservationId = ReservationId
            .newInstance(rm.getStartTime(), AM_ID);
      }
      AMDefinitionSynth amDef = AMDefinitionFactory.createFromSynth(job, slsRunner);
      startAMs(amDef, reservationId, job.getParams(), job.getDeadline());
    }
  }

  /**
   * Parse workload from a rumen trace file.
   */
  private void startAMFromRumenTrace(String inputTrace, long baselineTimeMS)
      throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    File fin = new File(inputTrace);

    try (JobTraceReader reader = new JobTraceReader(
        new Path(fin.getAbsolutePath()), conf)) {
      LoggedJob job = reader.getNext();

      while (job != null) {
        try {
          AMDefinitionRumen amDef =
              AMDefinitionFactory.createFromRumenTrace(job, baselineTimeMS,
                  slsRunner);
          startAMs(amDef);
        } catch (Exception e) {
          LOG.error("Failed to create an AM", e);
        }
        job = reader.getNext();
      }
    }
  }

  private void startAMs(AMDefinition amDef) {
    for (int i = 0; i < amDef.getJobCount(); i++) {
      JobDefinition jobDef = JobDefinition.Builder.create()
          .withAmDefinition(amDef)
          .withDeadline(-1)
          .withReservationId(null)
          .withParams(null)
          .build();
      runNewAM(jobDef);
    }
  }

  private void startAMs(AMDefinition amDef,
      ReservationId reservationId,
      Map<String, String> params, long deadline) {
    for (int i = 0; i < amDef.getJobCount(); i++) {
      JobDefinition jobDef = JobDefinition.Builder.create()
          .withAmDefinition(amDef)
          .withReservationId(reservationId)
          .withParams(params)
          .withDeadline(deadline)
          .build();
      runNewAM(jobDef);
    }
  }

  private void runNewAM(JobDefinition jobDef) {
    AMDefinition amDef = jobDef.getAmDefinition();
    String oldJobId = amDef.getOldAppId();
    AMSimulator amSim =
        createAmSimulator(amDef.getAmType());

    if (amSim != null) {
      int heartbeatInterval = conf.getInt(
          SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
          SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
      boolean isTracked = trackedApps.contains(oldJobId);

      if (oldJobId == null) {
        oldJobId = Integer.toString(AM_ID);
      }
      AM_ID++;
      amSim.init(amDef, rm, slsRunner, isTracked, runner.getStartTimeMS(), heartbeatInterval, appIdAMSim);
      if (jobDef.getReservationId() != null) {
        // if we have a ReservationId, delegate reservation creation to
        // AMSim (reservation shape is impl specific)
        UTCClock clock = new UTCClock();
        amSim.initReservation(jobDef.getReservationId(), jobDef.getDeadline(), clock.getTime());
      }
      runner.schedule(amSim);
      maxRuntime = Math.max(maxRuntime, amDef.getJobFinishTime());
      numTasks += amDef.getTaskContainers().size();
      amMap.put(oldJobId, amSim);
    }
  }

  private AMSimulator createAmSimulator(String jobType) {
    return (AMSimulator) ReflectionUtils.newInstance(
        amClassMap.get(jobType), new Configuration());
  }

  public AMSimulator getAMSimulator(ApplicationId appId) {
    return appIdAMSim.get(appId);
  }

  public void setInputType(TraceType inputType) {
    this.inputType = inputType;
  }

  public void setInputTraces(String[] inputTraces) {
    this.inputTraces = inputTraces.clone();
  }

  public void setResourceManager(ResourceManager rm) {
    this.rm = rm;
  }

  public Set<String> getTrackedApps() {
    return trackedApps;
  }

  public void setTrackedApps(Set<String> trackApps) {
    this.trackedApps = trackApps;
  }

  public int getNumAMs() {
    return numAMs;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public long getMaxRuntime() {
    return maxRuntime;
  }

  public Map<String, AMSimulator> getAmMap() {
    return amMap;
  }
}
