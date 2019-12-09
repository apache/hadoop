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

package org.apache.hadoop.yarn.sls.web;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.scheduler.FairSchedulerMetrics;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerMetrics;
import org.apache.hadoop.yarn.sls.scheduler.SchedulerWrapper;

import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

@Private
@Unstable
public class SLSWebApp extends HttpServlet {
  private static final long serialVersionUID = 1905162041950251407L;
  private transient Server server;
  private transient SchedulerWrapper wrapper;
  private transient MetricRegistry metrics;
  private transient SchedulerMetrics schedulerMetrics;
  // metrics objects
  private transient Gauge jvmFreeMemoryGauge;
  private transient Gauge jvmMaxMemoryGauge;
  private transient Gauge jvmTotalMemoryGauge;
  private transient Gauge numRunningAppsGauge;
  private transient Gauge numRunningContainersGauge;
  private transient Gauge allocatedMemoryGauge;
  private transient Gauge allocatedVCoresGauge;
  private transient Gauge availableMemoryGauge;
  private transient Gauge availableVCoresGauge;
  private transient Histogram allocateTimecostHistogram;
  private transient Histogram commitSuccessTimecostHistogram;
  private transient Histogram commitFailureTimecostHistogram;
  private transient Histogram handleTimecostHistogram;
  private transient Map<SchedulerEventType, Histogram>
     handleOperTimecostHistogramMap;
  private transient Map<String, Counter> queueAllocatedMemoryCounterMap;
  private transient Map<String, Counter> queueAllocatedVCoresCounterMap;
  private int port;
  private int ajaxUpdateTimeMS = 1000;
  // html page templates
  private String simulateInfoTemplate;
  private String simulateTemplate;
  private String trackTemplate;

  private transient Counter schedulerCommitSuccessCounter;
  private transient Counter schedulerCommitFailureCounter;
  private Long lastTrackingTime;
  private Long lastSchedulerCommitSuccessCount;
  private Long lastSchedulerCommitFailureCount;

  {
    // load templates
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      simulateInfoTemplate = IOUtils.toString(
          cl.getResourceAsStream("html/simulate.info.html.template"));
      simulateTemplate = IOUtils.toString(
          cl.getResourceAsStream("html/simulate.html.template"));
      trackTemplate = IOUtils.toString(
          cl.getResourceAsStream("html/track.html.template"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    in.defaultReadObject();
    handleOperTimecostHistogramMap = new HashMap<>();
    queueAllocatedMemoryCounterMap = new HashMap<>();
    queueAllocatedVCoresCounterMap = new HashMap<>();
  }

  public SLSWebApp(SchedulerWrapper wrapper, int metricsAddressPort) {
    this.wrapper = wrapper;
    handleOperTimecostHistogramMap = new HashMap<>();
    queueAllocatedMemoryCounterMap = new HashMap<>();
    queueAllocatedVCoresCounterMap = new HashMap<>();
    schedulerMetrics = wrapper.getSchedulerMetrics();
    metrics = schedulerMetrics.getMetrics();
    port = metricsAddressPort;
  }

  public void start() throws Exception {
    final ResourceHandler staticHandler = new ResourceHandler();
    staticHandler.setMimeTypes(new MimeTypes());
    String webRootDir = getClass().getClassLoader().getResource("html").
        toExternalForm();
    staticHandler.setResourceBase(webRootDir);

    Handler handler = new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest,
                         HttpServletRequest request,
                         HttpServletResponse response)
          throws IOException, ServletException {
        try{
          // timeunit
          int timeunit = 1000;   // second, divide millionsecond / 1000
          String timeunitLabel = "second";
          if (request.getParameter("u")!= null &&
                  request.getParameter("u").equalsIgnoreCase("m")) {
            timeunit = 1000 * 60;
            timeunitLabel = "minute";
          }

          // http request
          if (target.equals("/")) {
            printPageIndex(request, response);
          } else if (target.equals("/simulate")) {
            printPageSimulate(request, response, timeunit, timeunitLabel);
          } else if (target.equals("/track")) {
            printPageTrack(request, response, timeunit, timeunitLabel);
          } else
            // js/css request
            if (target.startsWith("/js") || target.startsWith("/css")) {
              response.setCharacterEncoding("utf-8");
              staticHandler.handle(target, baseRequest, request, response);
            } else
              // json request
              if (target.equals("/simulateMetrics")) {
                printJsonMetrics(request, response);
              } else if (target.equals("/trackMetrics")) {
                printJsonTrack(request, response);
              }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    server = new Server(port);
    server.setHandler(handler);

    server.start();
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * index html page, show simulation info
   * path ""
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageIndex(HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    String simulateInfo;
    if (SLSRunner.getSimulateInfoMap().isEmpty()) {
      String empty = "<tr><td colspan='2' align='center'>" +
              "No information available</td></tr>";
      simulateInfo = MessageFormat.format(simulateInfoTemplate, empty);
    } else {
      StringBuilder info = new StringBuilder();
      for (Map.Entry<String, Object> entry :
              SLSRunner.getSimulateInfoMap().entrySet()) {
        info.append("<tr>");
        info.append("<td class='td1'>").append(entry.getKey()).append("</td>");
        info.append("<td class='td2'>").append(entry.getValue())
                .append("</td>");
        info.append("</tr>");
      }
      simulateInfo =
              MessageFormat.format(simulateInfoTemplate, info.toString());
    }
    response.getWriter().println(simulateInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * simulate html page, show several real-runtime chart
   * path "/simulate"
   * use d3.js
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageSimulate(HttpServletRequest request,
                                 HttpServletResponse response, int timeunit,
                                 String timeunitLabel)
          throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    // queues {0}
    Set<String> queues = wrapper.getTracker().getQueueSet();
    StringBuilder queueInfo = new StringBuilder();

    int i = 0;
    for (String queue : queues) {
      queueInfo.append("legends[4][").append(i).append("] = 'queue.")
              .append(queue).append(".allocated.memory';");
      queueInfo.append("legends[5][").append(i).append("] = 'queue.")
              .append(queue).append(".allocated.vcores';");
      i ++;
    }

    // time unit label {1}
    // time unit {2}
    // ajax update time interval {3}
    String simulateInfo = MessageFormat.format(simulateTemplate,
            queueInfo.toString(), timeunitLabel, "" + timeunit,
            "" + ajaxUpdateTimeMS);
    response.getWriter().println(simulateInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * html page for tracking one queue or job
   * use d3.js
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printPageTrack(HttpServletRequest request,
                               HttpServletResponse response, int timeunit,
                               String timeunitLabel)
          throws IOException {
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);

    // tracked queues {0}
    StringBuilder trackedQueueInfo = new StringBuilder();
    Set<String> trackedQueues = wrapper.getTracker().getQueueSet();
    for(String queue : trackedQueues) {
      trackedQueueInfo.append("<option value='Queue ").append(queue)
              .append("'>").append(queue).append("</option>");
    }

    // tracked apps {1}
    StringBuilder trackedAppInfo = new StringBuilder();
    Set<String> trackedApps = wrapper.getTracker().getTrackedAppSet();
    for(String job : trackedApps) {
      trackedAppInfo.append("<option value='Job ").append(job)
              .append("'>").append(job).append("</option>");
    }

    // timeunit label {2}
    // time unit {3}
    // ajax update time {4}
    // final html
    String trackInfo = MessageFormat.format(trackTemplate,
            trackedQueueInfo.toString(), trackedAppInfo.toString(),
            timeunitLabel, "" + timeunit, "" + ajaxUpdateTimeMS);
    response.getWriter().println(trackInfo);

    ((Request) request).setHandled(true);
  }

  /**
   * package metrics information in a json and return
   * @param request http request
   * @param response http response
   * @throws java.io.IOException
   */
  private void printJsonMetrics(HttpServletRequest request,
                                HttpServletResponse response)
          throws IOException {
    response.setContentType("text/json");
    response.setStatus(HttpServletResponse.SC_OK);

    response.getWriter().println(generateRealTimeTrackingMetrics());
    ((Request) request).setHandled(true);
  }

  public String generateRealTimeTrackingMetrics() {
    // JVM
    double jvmFreeMemoryGB, jvmMaxMemoryGB, jvmTotalMemoryGB;
    if (jvmFreeMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.free.memory")) {
      jvmFreeMemoryGauge = metrics.getGauges().get("variable.jvm.free.memory");
    }
    if (jvmMaxMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.max.memory")) {
      jvmMaxMemoryGauge = metrics.getGauges().get("variable.jvm.max.memory");
    }
    if (jvmTotalMemoryGauge == null &&
            metrics.getGauges().containsKey("variable.jvm.total.memory")) {
      jvmTotalMemoryGauge = metrics.getGauges()
              .get("variable.jvm.total.memory");
    }
    jvmFreeMemoryGB = jvmFreeMemoryGauge == null ? 0 :
            Double.parseDouble(jvmFreeMemoryGauge.getValue().toString())
                    /1024/1024/1024;
    jvmMaxMemoryGB = jvmMaxMemoryGauge == null ? 0 :
            Double.parseDouble(jvmMaxMemoryGauge.getValue().toString())
                    /1024/1024/1024;
    jvmTotalMemoryGB = jvmTotalMemoryGauge == null ? 0 :
            Double.parseDouble(jvmTotalMemoryGauge.getValue().toString())
                    /1024/1024/1024;

    // number of running applications/containers
    String numRunningApps, numRunningContainers;
    if (numRunningAppsGauge == null &&
            metrics.getGauges().containsKey("variable.running.application")) {
      numRunningAppsGauge =
              metrics.getGauges().get("variable.running.application");
    }
    if (numRunningContainersGauge == null &&
            metrics.getGauges().containsKey("variable.running.container")) {
      numRunningContainersGauge =
              metrics.getGauges().get("variable.running.container");
    }
    numRunningApps = numRunningAppsGauge == null ? "0" :
            numRunningAppsGauge.getValue().toString();
    numRunningContainers = numRunningContainersGauge == null ? "0" :
            numRunningContainersGauge.getValue().toString();

    // cluster available/allocate resource
    double allocatedMemoryGB, allocatedVCoresGB,
            availableMemoryGB, availableVCoresGB;
    if (allocatedMemoryGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.allocated.memory")) {
      allocatedMemoryGauge = metrics.getGauges()
              .get("variable.cluster.allocated.memory");
    }
    if (allocatedVCoresGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.allocated.vcores")) {
      allocatedVCoresGauge = metrics.getGauges()
              .get("variable.cluster.allocated.vcores");
    }
    if (availableMemoryGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.available.memory")) {
      availableMemoryGauge = metrics.getGauges()
              .get("variable.cluster.available.memory");
    }
    if (availableVCoresGauge == null &&
            metrics.getGauges()
                    .containsKey("variable.cluster.available.vcores")) {
      availableVCoresGauge = metrics.getGauges()
              .get("variable.cluster.available.vcores");
    }
    allocatedMemoryGB = allocatedMemoryGauge == null ? 0 :
            Double.parseDouble(allocatedMemoryGauge.getValue().toString())/1024;
    allocatedVCoresGB = allocatedVCoresGauge == null ? 0 :
            Double.parseDouble(allocatedVCoresGauge.getValue().toString());
    availableMemoryGB = availableMemoryGauge == null ? 0 :
            Double.parseDouble(availableMemoryGauge.getValue().toString())/1024;
    availableVCoresGB = availableVCoresGauge == null ? 0 :
            Double.parseDouble(availableVCoresGauge.getValue().toString());

    // scheduler operation
    double allocateTimecost, commitSuccessTimecost, commitFailureTimecost,
        handleTimecost;
    if (allocateTimecostHistogram == null &&
            metrics.getHistograms().containsKey(
                    "sampler.scheduler.operation.allocate.timecost")) {
      allocateTimecostHistogram = metrics.getHistograms()
              .get("sampler.scheduler.operation.allocate.timecost");
    }
    if (commitSuccessTimecostHistogram == null &&
        metrics.getHistograms().containsKey(
            "sampler.scheduler.operation.commit.success.timecost")) {
      commitSuccessTimecostHistogram = metrics.getHistograms()
          .get("sampler.scheduler.operation.commit.success.timecost");
    }
    if (commitFailureTimecostHistogram == null &&
        metrics.getHistograms().containsKey(
            "sampler.scheduler.operation.commit.failure.timecost")) {
      commitFailureTimecostHistogram = metrics.getHistograms()
          .get("sampler.scheduler.operation.commit.failure.timecost");
    }
    if (handleTimecostHistogram == null &&
            metrics.getHistograms().containsKey(
                    "sampler.scheduler.operation.handle.timecost")) {
      handleTimecostHistogram = metrics.getHistograms()
              .get("sampler.scheduler.operation.handle.timecost");
    }
    allocateTimecost = allocateTimecostHistogram == null ? 0.0 :
            allocateTimecostHistogram.getSnapshot().getMean()/1000000;
    commitSuccessTimecost = commitSuccessTimecostHistogram == null ? 0.0 :
            commitSuccessTimecostHistogram.getSnapshot().getMean()/1000000;
    commitFailureTimecost = commitFailureTimecostHistogram == null ? 0.0 :
            commitFailureTimecostHistogram.getSnapshot().getMean()/1000000;
    handleTimecost = handleTimecostHistogram == null ? 0.0 :
            handleTimecostHistogram.getSnapshot().getMean()/1000000;
    // various handle operation
    Map<SchedulerEventType, Double> handleOperTimecostMap =
            new HashMap<SchedulerEventType, Double>();
    for (SchedulerEventType e : SchedulerEventType.values()) {
      String key = "sampler.scheduler.operation.handle." + e + ".timecost";
      if (! handleOperTimecostHistogramMap.containsKey(e) &&
              metrics.getHistograms().containsKey(key)) {
        handleOperTimecostHistogramMap.put(e, metrics.getHistograms().get(key));
      }
      double timecost = handleOperTimecostHistogramMap.containsKey(e) ?
          handleOperTimecostHistogramMap.get(e).getSnapshot().getMean()/1000000
              : 0;
      handleOperTimecostMap.put(e, timecost);
    }

    // allocated resource for each queue
    Map<String, Double> queueAllocatedMemoryMap = new HashMap<String, Double>();
    Map<String, Long> queueAllocatedVCoresMap = new HashMap<String, Long>();
    for (String queue : wrapper.getTracker().getQueueSet()) {
      // memory
      String key = "counter.queue." + queue + ".allocated.memory";
      if (! queueAllocatedMemoryCounterMap.containsKey(queue) &&
              metrics.getCounters().containsKey(key)) {
        queueAllocatedMemoryCounterMap.put(queue,
                metrics.getCounters().get(key));
      }
      double queueAllocatedMemoryGB =
              queueAllocatedMemoryCounterMap.containsKey(queue) ?
                  queueAllocatedMemoryCounterMap.get(queue).getCount()/1024.0
                      : 0;
      queueAllocatedMemoryMap.put(queue, queueAllocatedMemoryGB);
      // vCores
      key = "counter.queue." + queue + ".allocated.cores";
      if (! queueAllocatedVCoresCounterMap.containsKey(queue) &&
              metrics.getCounters().containsKey(key)) {
        queueAllocatedVCoresCounterMap.put(
                queue, metrics.getCounters().get(key));
      }
      long queueAllocatedVCores =
              queueAllocatedVCoresCounterMap.containsKey(queue) ?
                      queueAllocatedVCoresCounterMap.get(queue).getCount(): 0;
      queueAllocatedVCoresMap.put(queue, queueAllocatedVCores);
    }

    // calculate commit throughput, unit is number/second
    if (schedulerCommitSuccessCounter == null && metrics.getCounters()
        .containsKey("counter.scheduler.operation.commit.success")) {
      schedulerCommitSuccessCounter = metrics.getCounters()
          .get("counter.scheduler.operation.commit.success");
    }
    if (schedulerCommitFailureCounter == null && metrics.getCounters()
        .containsKey("counter.scheduler.operation.commit.failure")) {
      schedulerCommitFailureCounter = metrics.getCounters()
          .get("counter.scheduler.operation.commit.failure");
    }
    long schedulerCommitSuccessThroughput = 0;
    long schedulerCommitFailureThroughput = 0;
    if (schedulerCommitSuccessCounter != null
        && schedulerCommitFailureCounter != null) {
      long currentTrackingTime = System.currentTimeMillis();
      long currentSchedulerCommitSucessCount =
          schedulerCommitSuccessCounter.getCount();
      long currentSchedulerCommitFailureCount =
          schedulerCommitFailureCounter.getCount();
      if (lastTrackingTime != null) {
        double intervalSeconds =
            (double) (currentTrackingTime - lastTrackingTime) / 1000;
        schedulerCommitSuccessThroughput = Math.round(
            (currentSchedulerCommitSucessCount
                - lastSchedulerCommitSuccessCount) / intervalSeconds);
        schedulerCommitFailureThroughput = Math.round(
            (currentSchedulerCommitFailureCount
                - lastSchedulerCommitFailureCount) / intervalSeconds);
      }
      lastTrackingTime = currentTrackingTime;
      lastSchedulerCommitSuccessCount = currentSchedulerCommitSucessCount;
      lastSchedulerCommitFailureCount = currentSchedulerCommitFailureCount;
    }

    // package results
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"time\":" ).append(System.currentTimeMillis())
            .append(",\"jvm.free.memory\":").append(jvmFreeMemoryGB)
            .append(",\"jvm.max.memory\":").append(jvmMaxMemoryGB)
            .append(",\"jvm.total.memory\":").append(jvmTotalMemoryGB)
            .append(",\"running.applications\":").append(numRunningApps)
            .append(",\"running.containers\":").append(numRunningContainers)
            .append(",\"cluster.allocated.memory\":").append(allocatedMemoryGB)
            .append(",\"cluster.allocated.vcores\":").append(allocatedVCoresGB)
            .append(",\"cluster.available.memory\":").append(availableMemoryGB)
            .append(",\"cluster.available.vcores\":").append(availableVCoresGB);

    for (String queue : wrapper.getTracker().getQueueSet()) {
      sb.append(",\"queue.").append(queue).append(".allocated.memory\":")
              .append(queueAllocatedMemoryMap.get(queue));
      sb.append(",\"queue.").append(queue).append(".allocated.vcores\":")
              .append(queueAllocatedVCoresMap.get(queue));
    }
    // scheduler allocate & handle
    sb.append(",\"scheduler.allocate.timecost\":").append(allocateTimecost);
    sb.append(",\"scheduler.commit.success.timecost\":")
        .append(commitSuccessTimecost);
    sb.append(",\"scheduler.commit.failure.timecost\":")
        .append(commitFailureTimecost);
    sb.append(",\"scheduler.commit.success.throughput\":")
        .append(schedulerCommitSuccessThroughput);
    sb.append(",\"scheduler.commit.failure.throughput\":")
        .append(schedulerCommitFailureThroughput);
    sb.append(",\"scheduler.handle.timecost\":").append(handleTimecost);
    for (SchedulerEventType e : SchedulerEventType.values()) {
      sb.append(",\"scheduler.handle-").append(e).append(".timecost\":")
              .append(handleOperTimecostMap.get(e));
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * package metrics information for one tracked queue/app
   * only support FairScheduler currently
   * @throws java.io.IOException
   */
  private void printJsonTrack(HttpServletRequest request,
                              HttpServletResponse response) throws IOException {
    response.setContentType("text/json");
    response.setStatus(HttpServletResponse.SC_OK);

    StringBuilder sb = new StringBuilder();
    if(schedulerMetrics instanceof FairSchedulerMetrics) {
      String para = request.getParameter("t");
      if (para.startsWith("Job ")) {
        String appId = para.substring("Job ".length());

        sb.append("{");
        sb.append("\"time\": ").append(System.currentTimeMillis()).append(",");
        sb.append("\"appId\": \"").append(appId).append("\"");
        for(String metric : this.schedulerMetrics.getAppTrackedMetrics()) {
          String key = "variable.app." + appId + "." + metric;
          sb.append(",\"").append(metric).append("\": ");
          if (metrics.getGauges().containsKey(key)) {
            double memoryGB =
                    Double.parseDouble(
                            metrics.getGauges().get(key).getValue().toString())
                            / 1024;
            sb.append(memoryGB);
          } else {
            sb.append(-1);
          }
        }
        sb.append("}");

      } else if(para.startsWith("Queue ")) {
        String queueName = para.substring("Queue ".length());
        sb.append("{");
        sb.append("\"time\": ").append(System.currentTimeMillis()).append(",");
        sb.append("\"queueName\": \"").append(queueName).append("\"");
        for(String metric : this.schedulerMetrics.getQueueTrackedMetrics()) {
          String key = "variable.queue." + queueName + "." + metric;
          sb.append(",\"").append(metric).append("\": ");
          if (metrics.getGauges().containsKey(key)) {
            double memoryGB =
                    Double.parseDouble(
                            metrics.getGauges().get(key).getValue().toString())
                            / 1024;
            sb.append(memoryGB);
          } else {
            sb.append(-1);
          }
        }
        sb.append("}");
      }
    }
    String output = sb.toString();
    if (output.isEmpty()) {
      output = "[]";
    }
    response.getWriter().println(output);
    // package result
    ((Request) request).setHandled(true);
  }
}
