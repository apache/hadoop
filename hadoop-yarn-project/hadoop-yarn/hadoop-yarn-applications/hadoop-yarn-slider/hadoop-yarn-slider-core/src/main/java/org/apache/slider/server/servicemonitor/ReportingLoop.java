/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * This is the monitor service
 */
public final class ReportingLoop implements Runnable, ProbeReportHandler, MonitorKeys, Closeable {
  protected static final Logger log = LoggerFactory.getLogger(ReportingLoop.class);
  private final ProbeWorker worker;
  private final Thread workerThread;
  private final int reportInterval;
  private final int probeTimeout;
  private final int bootstrapTimeout;
  private ProbeReportHandler reporter;
  private final String name;
  private volatile boolean mustExit;

  public ReportingLoop(String name,
                       ProbeReportHandler reporter,
                       List<Probe> probes,
                       List<Probe> dependencyProbes,
                       int probeInterval,
                       int reportInterval,
                       int probeTimeout,
                       int bootstrapTimeout) throws IOException {
    this(name,
         reporter,
         new ProbeWorker(probes, dependencyProbes, probeInterval, bootstrapTimeout),
         reportInterval,
         probeTimeout);
  }

  /**
   * Create a new reporting loop -and bond the worker's ProbeReportHandler
   * to us
   * @param name
   * @param reporter
   * @param worker
   * @param reportInterval
   * @param probeTimeout
   */
  public ReportingLoop(String name,
                       ProbeReportHandler reporter,
                       ProbeWorker worker,
                       int reportInterval,
                       int probeTimeout) throws IOException {
    this.name = name;
    this.reporter = reporter;
    this.reportInterval = reportInterval;
    this.probeTimeout = probeTimeout;
    this.worker = worker;
    this.bootstrapTimeout = worker.getBootstrapTimeout();
    worker.setReportHandler(this);
    workerThread = new Thread(worker, "probe thread - " + name);
    worker.init();
  }
  
  public int getBootstrapTimeout() {
    return bootstrapTimeout;
  }

  public ReportingLoop withReporter(ProbeReportHandler reporter) {
    assert this.reporter == null : "attempting to reassign reporter ";
    assert reporter != null : "new reporter is null";
    this.reporter = reporter;
    return this;
  }

  /**
   * Start the monitoring.
   *
   * @return false if the monitoring did not start and that the worker threads
   *         should be run up.
   */
  public boolean startReporting() {
    String description = "Service Monitor for " + name + ", probe-interval= "
                         + MonitorUtils.millisToHumanTime(worker.interval)
                         + ", report-interval=" + MonitorUtils.millisToHumanTime(reportInterval)
                         + ", probe-timeout=" + timeoutToStr(probeTimeout)
                         + ", bootstrap-timeout=" + timeoutToStr(bootstrapTimeout);
    log.info("Starting reporting"
             + " to " + reporter
             + description);
    return reporter.commence(name, description);
  }

  private String timeoutToStr(int timeout) {
    return timeout >= 0 ? MonitorUtils.millisToHumanTime(timeout) : "not set";
  }

  private void startWorker() {
    log.info("Starting reporting worker thread ");
    workerThread.setDaemon(true);
    workerThread.start();
  }


  /**
   * This exits the process cleanly
   */
  @Override
  public void close() {
    log.info("Stopping reporting");
    mustExit = true;
    if (worker != null) {
      worker.setMustExit();
      workerThread.interrupt();
    }
    if (reporter != null) {
      reporter.unregister();
    }
  }

  @Override
  public void probeFailure(ProbeFailedException exception) {
    reporter.probeFailure(exception);
  }

  @Override
  public void probeProcessStateChange(ProbePhase probePhase) {
    reporter.probeProcessStateChange(probePhase);
  }

  @Override
  public void probeBooted(ProbeStatus status) {
    reporter.probeBooted(status);
  }

  private long now() {
    return System.currentTimeMillis();
  }

  @Override
  public void probeResult(ProbePhase phase, ProbeStatus status) {
    reporter.probeResult(phase, status);
  }

  @Override
  public boolean commence(String n, String description) {
    return true;
  }

  @Override
  public void unregister() {
  }

  @Override
  public void heartbeat(ProbeStatus status) {
  }

  @Override
  public void probeTimedOut(ProbePhase currentPhase, Probe probe, ProbeStatus lastStatus,
      long currentTime) {
  }

  @Override
  public void liveProbeCycleCompleted() {
    //delegate to the reporter
    reporter.liveProbeCycleCompleted();
  }

  /**
   * The reporting loop
   */
  void reportingLoop() {

    while (!mustExit) {
      try {
        ProbeStatus workerStatus = worker.getLastStatus();
        long now = now();
        long lastStatusIssued = workerStatus.getTimestamp();
        long timeSinceLastStatusIssued = now - lastStatusIssued;
        //two actions can occur here: a heartbeat is issued or a timeout reported. 
        //this flag decides which
        boolean heartbeat;

        //based on phase, decide whether to heartbeat or timeout
        ProbePhase probePhase = worker.getProbePhase();
        switch (probePhase) {
          case DEPENDENCY_CHECKING:
            //no timeouts in dependency phase
            heartbeat = true;
            break;

          case BOOTSTRAPPING:
            //the timeout here is fairly straightforward: heartbeats are
            //raised while the worker hasn't timed out
            heartbeat = bootstrapTimeout < 0 || timeSinceLastStatusIssued < bootstrapTimeout;

            break;

          case LIVE:
            //use the probe timeout interval between the current time
            //and the time the last status event was received.
            heartbeat = timeSinceLastStatusIssued < probeTimeout;
            break;

          case INIT:
          case TERMINATING:
          default:
            //send a heartbeat, because this isn't the time to be failing
            heartbeat = true;
        }
        if (heartbeat) {
          //a heartbeat is sent to the reporter
          reporter.heartbeat(workerStatus);
        } else {
          //no response from the worker -it is hung.
          reporter.probeTimedOut(probePhase,
                                 worker.getCurrentProbe(),
                                 workerStatus,
                                 now
                                );
        }

        //now sleep
        Thread.sleep(reportInterval);

      } catch (InterruptedException e) {
        //interrupted -always exit the loop.
        break;
      }
    }
    //this point is reached if and only if a clean exit was requested or something failed.
  }

  /**
   * This can be run in a separate thread, or it can be run directly from the caller.
   * Test runs do the latter, HAM runs multiple reporting threads.
   */
  @Override
  public void run() {
    try {
      startWorker();
      reportingLoop();
    } catch (RuntimeException e) {
      log.warn("Failure in the reporting loop: " + e, e);
      //rethrow so that inline code can pick it up (e.g. test runs)
      throw e;
    }
  }
}
