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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the entry point to do work. A list of probes is taken in, in order of
 * booting. Once live they go to the live probes list.
 *
 * The dependency probes are a set of probes for dependent services, all of which
 * must be live before boot probes commence.
 *
 * The boot probes are executed and are allowed to fail; failure is interpreted as "not yet live"
 *
 * Once all boot probes are live, the live list is used for probes; these must not fail.
 *
 * There is no timeout on dependency probe bootstrap time, because of the notion that
 * restarting this service will have no effect on the dependencies. 
 */

public class ProbeWorker implements Runnable {
  protected static final Logger log = LoggerFactory.getLogger(ProbeWorker.class);

  public static final String FAILED_TO_BOOT = "Monitored service failed to bootstrap after ";
  public static final String FAILURE_OF_A_LIVE_PROBE_DURING_BOOTSTRAPPING = "Failure of a live probe during bootstrapping";
  private final List<Probe> monitorProbes;
  private final List<Probe> dependencyProbes;
  public final int interval;
  protected volatile ProbeStatus lastStatus;
  protected volatile ProbeStatus lastFailingBootstrapProbe;
  protected volatile Probe currentProbe;
  private volatile boolean mustExit;
  private final int bootstrapTimeout;
  private long bootstrapEndtime;

  private ProbeReportHandler reportHandler;
  private volatile ProbePhase probePhase = ProbePhase.INIT;

  /**
   * Create a probe worker
   * @param monitorProbes list of probes that must boot and then go live -after which
   * they must stay live.
   * @param dependencyProbes the list of dependency probes that must all succeed before
   * any attempt to probe the direct probe list is performed. Once the 
   * dependency phase has completed, these probes are never checked again.
   * @param interval probe interval in milliseconds.
   * @param bootstrapTimeout timeout for bootstrap in milliseconds
   */
  public ProbeWorker(List<Probe> monitorProbes, List<Probe> dependencyProbes, int interval, int bootstrapTimeout) {
    this.monitorProbes = monitorProbes;
    this.dependencyProbes = dependencyProbes != null ? dependencyProbes : new ArrayList<Probe>(0);
    this.interval = interval;
    lastStatus = new ProbeStatus(now(),
                                 "Initial status");
    lastStatus.setProbePhase(ProbePhase.INIT);
    this.bootstrapTimeout = bootstrapTimeout;
  }

  public void init() throws IOException {
    for (Probe probe : monitorProbes) {
      probe.init();
    }
    for (Probe probe : dependencyProbes) {
      probe.init();
    }
  }

  public void setReportHandler(ProbeReportHandler reportHandler) {
    this.reportHandler = reportHandler;
  }

  public void setMustExit() {
    this.mustExit = true;
  }

  public ProbeStatus getLastStatus() {
    return lastStatus;
  }

  public synchronized Probe getCurrentProbe() {
    return currentProbe;
  }

  public ProbePhase getProbePhase() {
    return probePhase;
  }

  /**
   * Enter the new process state, and report it to the report handler.
   * This is synchronized just to make sure there isn't more than one
   * invocation at the same time.
   * @param status the new process status
   */
  private synchronized void enterProbePhase(ProbePhase status) {
    this.probePhase = status;
    if (reportHandler != null) {
      reportHandler.probeProcessStateChange(status);
    }
  }

  /**
   * Report the probe status to the listener -setting the probe phase field
   * before doing so.
   * The value is also stored in the {@link #lastStatus} field
   * @param status the new status
   */
  private void reportProbeStatus(ProbeStatus status) {
    ProbePhase phase = getProbePhase();
    status.setProbePhase(phase);
    lastStatus = status;
    reportHandler.probeResult(phase, status);
  }

  /**
   * Ping one probe. Logs the operation at debug level; sets the field <code>currentProbe</code>
   * to the probe for the duration of the operation -this is used when identifying the
   * cause of a hung reporting loop
   * @param probe probe to ping
   * @param live flag to indicate whether or not the operation is live or bootstrapping
   * @return the status of the ping
   * @throws ProbeInterruptedException if the probe has been told to exit
   */
  private ProbeStatus ping(Probe probe, boolean live) throws ProbeInterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("Executing " + probe);
    }
    checkForExitRequest();
    currentProbe = probe;
    try {
      return probe.ping(live);
    } finally {
      currentProbe = null;
    }
  }

  /**
   * Check for an exit request -and convert it to an exception if made
   * @throws ProbeInterruptedException iff {@link #mustExit} is true
   */
  private void checkForExitRequest() throws ProbeInterruptedException {
    if (mustExit) {
      throw new ProbeInterruptedException();
    }
  }

  /**
   * Check the dependencies. 
   * The moment a failing test is reached the call returns without
   * any reporting.
   *
   * All successful probes are reported, so as to keep the heartbeats happy.
   *
   * @return the status of the last dependency check. If this is a success
   * them every probe passed.
   */
  private ProbeStatus checkDependencyProbes() throws ProbeInterruptedException {
    ProbeStatus status = null;
    for (Probe dependency : dependencyProbes) {
      //ping them, making clear they are not to run any bootstrap logic
      status = ping(dependency, true);

      if (!status.isSuccess()) {
        //the first failure means the rest of the list can be skipped
        break;
      }
      reportProbeStatus(status);
    }
    //return the last status
    return status;
  }

  /**
   * Run through all the dependency probes and report their outcomes successes (even if they fail)
   * @return true iff all the probes have succeeded.
   * @throws ProbeInterruptedException if the process was interrupted.
   */
  public boolean checkAndReportDependencyProbes() throws ProbeInterruptedException {
    ProbeStatus status;
    status = checkDependencyProbes();
    if (status != null && !status.isSuccess()) {
      //during dependency checking, a failure is still reported as a success
      status.markAsSuccessful();
      reportProbeStatus(status);
      //then return without checking anything else
      return false;
    }
    //all dependencies are done.
    return true;
  }

  /**
   * Begin bootstrapping by telling each probe that they have started.
   * This sets the timeouts up, as well as permits any other set-up actions
   * to begin.
   */
  private void beginBootstrapProbes() {
    synchronized (this) {
      bootstrapEndtime = now() + bootstrapTimeout;
    }
    for (Probe probe : monitorProbes) {
      probe.beginBootstrap();
    }
  }

  private long now() {
    return System.currentTimeMillis();
  }


  /**
   * Check the bootstrap probe list. All successful probes get reported.
   * The first unsuccessful probe will be returned and not reported (left for policy upstream).
   * If the failing probe has timed out, that is turned into a {@link ProbeFailedException}
   * @return the last (unsuccessful) probe, or null if they all succeeded
   * @throws ProbeInterruptedException interrupts
   * @throws ProbeFailedException on a boot timeout
   */
  private boolean checkBootstrapProbes() throws ProbeInterruptedException, ProbeFailedException {
    verifyBootstrapHasNotTimedOut();

    boolean probeFailed = false;
    //now run through all the bootstrap probes
    for (Probe probe : monitorProbes) {
      //ping them
      ProbeStatus status = ping(probe, false);
      if (!status.isSuccess()) {
        probeFailed = true;
        lastFailingBootstrapProbe = status;
        probe.failureCount++;
        if (log.isDebugEnabled()) {
          log.debug("Booting probe failed: " + status);
        }
        //at this point check to see if the timeout has occurred -and if so, force in the last probe status.

        //this is a failure but not a timeout
        //during boot, a failure of a probe that hasn't booted is still reported as a success
        if (!probe.isBooted()) {
          //so the success bit is flipped
          status.markAsSuccessful();
          reportProbeStatus(status);
        } else {
          //the probe had booted but then it switched to failing

          //update the status unedited
          reportProbeStatus(status);
          //then fail
          throw raiseProbeFailure(status, FAILURE_OF_A_LIVE_PROBE_DURING_BOOTSTRAPPING);
        }
      } else {
        //this probe is working
        if (!probe.isBooted()) {
          //if it is new, mark it as live
          if (log.isDebugEnabled()) {
            log.debug("Booting probe is now live: " + probe);
          }
          probe.endBootstrap();
          //tell the report handler that another probe has booted
          reportHandler.probeBooted(status);
        }
        //push out its status
        reportProbeStatus(status);
        probe.successCount++;
      }
    }
    return !probeFailed;
  }


  public int getBootstrapTimeout() {
    return bootstrapTimeout;
  }

  /**
   * This checks that bootstrap operations have not timed out
   * @throws ProbeFailedException if the bootstrap has failed
   */
  public void verifyBootstrapHasNotTimedOut() throws ProbeFailedException {
    //first step -look for a timeout
    if (isBootstrapTimeExceeded()) {
      String text = FAILED_TO_BOOT
                    + MonitorUtils.millisToHumanTime(bootstrapTimeout);

      ProbeStatus status;
      if (lastFailingBootstrapProbe != null) {
        status = lastFailingBootstrapProbe;
        status.setSuccess(false);
      } else {
        status = new ProbeStatus();
        status.finish(null, false, text, null);
      }

      throw raiseProbeFailure(status,
                              text);
    }
  }

  /**
   * predicate that gets current time and checks for its time being exceeded.
   * @return true iff the current time is > the end time
   */
  public synchronized boolean isBootstrapTimeExceeded() {
    return now() > bootstrapEndtime;
  }

  /**
   * run through all the bootstrap probes and see if they are live.
   * @return true iff all boot probes succeeded
   * @throws ProbeInterruptedException the probe interruption flags
   * @throws ProbeFailedException if a probe failed.
   */
  public boolean checkAndReportBootstrapProbes() throws ProbeInterruptedException,
                                                        ProbeFailedException {
    if (bootstrapTimeout <= 0) {
      //there is no period of grace for bootstrapping probes, so return true saying
      //this phase is complete
      return true;
    }
    //now the bootstrapping probes
    return checkBootstrapProbes();
  }


  /**
   * run through all the live probes, pinging and reporting them.
   * A single probe failure is turned into an exception
   * @throws ProbeFailedException a probe failed
   * @throws ProbeInterruptedException the probe process was explicitly interrupted
   */
  protected void checkAndReportLiveProbes() throws ProbeFailedException, ProbeInterruptedException {
    ProbeStatus status = null;
    //go through the live list
    if (log.isDebugEnabled()) {
      log.debug("Checking live probes");
    }
    for (Probe probe : monitorProbes) {
      status = ping(probe, true);
      reportProbeStatus(status);
      if (!status.isSuccess()) {
        throw raiseProbeFailure(status, "Failure of probe in \"live\" monitor");
      }
      probe.successCount++;
    }
    //here all is well, so notify the reporter
    reportHandler.liveProbeCycleCompleted();
  }

  /**
   * Run the set of probes relevant for this phase of the probe lifecycle.
   * @throws ProbeFailedException a probe failed
   * @throws ProbeInterruptedException the probe process was explicitly interrupted
   */
  protected void executeProbePhases() throws ProbeFailedException, ProbeInterruptedException {
    switch (probePhase) {
      case INIT:
        enterProbePhase(ProbePhase.DEPENDENCY_CHECKING);
        //fall through straight into the dependency check
      case DEPENDENCY_CHECKING:
        if (checkAndReportDependencyProbes()) {
          enterProbePhase(ProbePhase.BOOTSTRAPPING);
          beginBootstrapProbes();
        }
        break;
      case BOOTSTRAPPING:
        if (checkAndReportBootstrapProbes()) {
          enterProbePhase(ProbePhase.LIVE);
        }
        break;
      case LIVE:
        checkAndReportLiveProbes();
        break;

      case TERMINATING:
      default:
        //do nothing.
        break;
    }
  }


  /**
   * Raise a probe failure; injecting the phase into the status result first
   *
   * @param status ping result
   * @param text optional text -null or "" means "none"
   * @return an exception ready to throw
   */
  private ProbeFailedException raiseProbeFailure(ProbeStatus status, String text) {
    status.setProbePhase(probePhase);
    log.info("Probe failed: " + status);
    return new ProbeFailedException(text, status);
  }

  @Override
  public void run() {
    int size = monitorProbes.size();
    log.info("Probe Worker Starting; " + size + " probe" + MonitorUtils.toPlural(size) + ":");
    enterProbePhase(ProbePhase.DEPENDENCY_CHECKING);
    for (Probe probe : monitorProbes) {
      log.info(probe.getName());
    }
    while (!mustExit) {
      try {
        Thread.sleep(interval);
        executeProbePhases();
      } catch (ProbeFailedException e) {
        //relay to the inner loop handler
        probeFailed(e);
      } catch (InterruptedException interrupted) {
        break;
      } catch (ProbeInterruptedException e) {
        //exit raised.
        //this will be true, just making extra-sure
        break;
      }
    }
    log.info("Probe Worker Exiting");
    enterProbePhase(ProbePhase.TERMINATING);
  }


  protected void probeFailed(ProbeFailedException e) {
    reportHandler.probeFailure(e);
  }

}
