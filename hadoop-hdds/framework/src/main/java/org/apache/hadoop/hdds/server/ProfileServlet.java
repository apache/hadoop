/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet that runs async-profiler as web-endpoint.
 * <p>
 * Source: https://github.com/apache/hive/blob/master/common/src/java/org
 * /apache/hive/http/ProfileServlet.java
 * <p>
 * Following options from async-profiler can be specified as query paramater.
 * //  -e event          profiling event: cpu|alloc|lock|cache-misses etc.
 * //  -d duration       run profiling for <duration> seconds (integer)
 * //  -i interval       sampling interval in nanoseconds (long)
 * //  -j jstackdepth    maximum Java stack depth (integer)
 * //  -b bufsize        frame buffer size (long)
 * //  -t                profile different threads separately
 * //  -s                simple class names instead of FQN
 * //  -o fmt[,fmt...]   output format:
 * summary|traces|flat|collapsed|svg|tree|jfr
 * //  --width px        SVG width pixels (integer)
 * //  --height px       SVG frame height pixels (integer)
 * //  --minwidth px     skip frames smaller than px (double)
 * //  --reverse         generate stack-reversed FlameGraph / Call tree
 * Example:
 * - To collect 30 second CPU profile of current process (returns FlameGraph
 * svg)
 * curl "http://localhost:10002/prof"
 * - To collect 1 minute CPU profile of current process and output in tree
 * format (html)
 * curl "http://localhost:10002/prof?output=tree&duration=60"
 * - To collect 30 second heap allocation profile of current process (returns
 * FlameGraph svg)
 * curl "http://localhost:10002/prof?event=alloc"
 * - To collect lock contention profile of current process (returns
 * FlameGraph svg)
 * curl "http://localhost:10002/prof?event=lock"
 * Following event types are supported (default is 'cpu') (NOTE: not all
 * OS'es support all events)
 * // Perf events:
 * //    cpu
 * //    page-faults
 * //    context-switches
 * //    cycles
 * //    instructions
 * //    cache-references
 * //    cache-misses
 * //    branches
 * //    branch-misses
 * //    bus-cycles
 * //    L1-dcache-load-misses
 * //    LLC-load-misses
 * //    dTLB-load-misses
 * //    mem:breakpoint
 * //    trace:tracepoint
 * // Java events:
 * //    alloc
 * //    lock
 */
public class ProfileServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(ProfileServlet.class);
  private static final String ACCESS_CONTROL_ALLOW_METHODS =
      "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN =
      "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
  private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
  private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY =
      "async.profiler.home";
  private static final String PROFILER_SCRIPT = "/profiler.sh";
  private static final int DEFAULT_DURATION_SECONDS = 10;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);
  static final Path OUTPUT_DIR =
      Paths.get(System.getProperty("java.io.tmpdir"), "prof-output");

  private Lock profilerLock = new ReentrantLock();
  private Integer pid;
  private String asyncProfilerHome;
  private transient Process process;

  public ProfileServlet() {
    this.asyncProfilerHome = getAsyncProfilerHome();
    this.pid = getPid();
    LOG.info("Servlet process PID: {} asyncProfilerHome: {}", pid,
        asyncProfilerHome);
    try {
      Files.createDirectories(OUTPUT_DIR);
    } catch (IOException e) {
      LOG.error(
          "Can't create the output directory for java profiler: " + OUTPUT_DIR,
          e);
    }
  }

  private Integer getPid() {
    // JVM_PID is exported by bin/ozone
    String pidStr = System.getenv("JVM_PID");

    // in case if it is not set correctly used fallback from mxbean which is
    // implementation specific
    if (pidStr == null || pidStr.trim().isEmpty()) {
      String name = ManagementFactory.getRuntimeMXBean().getName();
      if (name != null) {
        int idx = name.indexOf("@");
        if (idx != -1) {
          pidStr = name.substring(0, name.indexOf("@"));
        }
      }
    }
    try {
      if (pidStr != null) {
        return Integer.valueOf(pidStr);
      }
    } catch (NumberFormatException nfe) {
      // ignore
    }
    return null;
  }

  public Process runCmdAsync(List<String> cmd) {
    try {
      LOG.info("Running command async: " + cmd);
      return new ProcessBuilder(cmd).inheritIO().start();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  protected void doGet(final HttpServletRequest req,
      final HttpServletResponse resp) throws IOException {
    // make sure async profiler home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter().write("ASYNC_PROFILER_HOME env is not set.");
      return;
    }

    //download the finished file
    if (req.getParameter("file") != null) {
      doGetDownload(req.getParameter("file"), req, resp);
      return;
    }
    // if pid is explicitly specified, use it else default to current process
    pid = getInteger(req, "pid", pid);

    // if pid is not specified in query param and if current process pid
    // cannot be determined
    if (pid == null) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter().write(
          "'pid' query parameter unspecified or unable to determine PID of "
              + "current process.");
      return;
    }

    final int duration = getInteger(req, "duration", DEFAULT_DURATION_SECONDS);
    final Output output = getOutput(req);
    final Event event = getEvent(req);
    final Long interval = getLong(req, "interval");
    final Integer jstackDepth = getInteger(req, "jstackdepth", null);
    final Long bufsize = getLong(req, "bufsize");
    final boolean thread = req.getParameterMap().containsKey("thread");
    final boolean simple = req.getParameterMap().containsKey("simple");
    final Integer width = getInteger(req, "width", null);
    final Integer height = getInteger(req, "height", null);
    final Double minwidth = getMinWidth(req);
    final boolean reverse = req.getParameterMap().containsKey("reverse");

    if (process == null || !process.isAlive()) {
      try {
        int lockTimeoutSecs = 3;
        if (profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS)) {
          try {
            File outputFile =
                OUTPUT_DIR.resolve("async-prof-pid-" + pid + "-" +
                    event.name().toLowerCase() + "-" + ID_GEN.incrementAndGet()
                    + "." +
                    output.name().toLowerCase()).toFile();
            List<String> cmd = new ArrayList<>();
            cmd.add(asyncProfilerHome + PROFILER_SCRIPT);
            cmd.add("-e");
            cmd.add(event.getInternalName());
            cmd.add("-d");
            cmd.add("" + duration);
            cmd.add("-o");
            cmd.add(output.name().toLowerCase());
            cmd.add("-f");
            cmd.add(outputFile.getAbsolutePath());
            if (interval != null) {
              cmd.add("-i");
              cmd.add(interval.toString());
            }
            if (jstackDepth != null) {
              cmd.add("-j");
              cmd.add(jstackDepth.toString());
            }
            if (bufsize != null) {
              cmd.add("-b");
              cmd.add(bufsize.toString());
            }
            if (thread) {
              cmd.add("-t");
            }
            if (simple) {
              cmd.add("-s");
            }
            if (width != null) {
              cmd.add("--width");
              cmd.add(width.toString());
            }
            if (height != null) {
              cmd.add("--height");
              cmd.add(height.toString());
            }
            if (minwidth != null) {
              cmd.add("--minwidth");
              cmd.add(minwidth.toString());
            }
            if (reverse) {
              cmd.add("--reverse");
            }
            cmd.add(pid.toString());
            process = runCmdAsync(cmd);

            // set response and set refresh header to output location
            setResponseHeader(resp);
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            String relativeUrl = "/prof?file=" + outputFile.getName();
            resp.getWriter().write(
                "Started [" + event.getInternalName()
                    + "] profiling. This page will automatically redirect to " +
                    relativeUrl + " after " + duration
                    + " seconds.\n\ncommand:\n" + Joiner.on(" ").join(cmd));
            resp.getWriter().write(
                "\n\n\nPlease make sure that you enabled the profiling on "
                    + "kernel level:\n"
                    + "echo 1 > /proc/sys/kernel/perf_event_paranoid\n"
                    + "echo 0 > /proc/sys/kernel/kptr_restrict\n\n"
                    + "See https://github"
                    + ".com/jvm-profiling-tools/async-profiler#basic-usage"
                    + " for more details.");
            // to avoid auto-refresh by ProfileOutputServlet, refreshDelay
            // can be specified via url param
            int refreshDelay = getInteger(req, "refreshDelay", 0);

            // instead of sending redirect, set auto-refresh so that browsers
            // will refresh with redirected url
            resp.setHeader("Refresh",
                (duration + refreshDelay) + ";" + relativeUrl);
            resp.getWriter().flush();
          } finally {
            profilerLock.unlock();
          }
        } else {
          setResponseHeader(resp);
          resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          resp.getWriter().write(
              "Unable to acquire lock. Another instance of profiler might be "
                  + "running.");
          LOG.warn(
              "Unable to acquire lock in {} seconds. Another instance of "
                  + "profiler might be running.",
              lockTimeoutSecs);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while acquiring profile lock.", e);
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } else {
      setResponseHeader(resp);
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resp.getWriter()
          .write("Another instance of profiler is already running.");
    }
  }

  protected void doGetDownload(String fileName, final HttpServletRequest req,
      final HttpServletResponse resp)
      throws IOException {

    File requestedFile =
        ProfileServlet.OUTPUT_DIR.resolve(fileName).toAbsolutePath()
            .toFile();
    // async-profiler version 1.4 writes 'Started [cpu] profiling' to output
    // file when profiler is running which
    // gets replaced by final output. If final output is not ready yet, the
    // file size will be <100 bytes (in all modes).
    if (requestedFile.length() < 100) {
      LOG.info("{} is incomplete. Sending auto-refresh header..",
          requestedFile);
      resp.setHeader("Refresh",
          "2," + req.getRequestURI() + "?file=" + fileName);
      resp.getWriter().write(
          "This page will auto-refresh every 2 second until output file is "
              + "ready..");
    } else {
      if (fileName.endsWith(".svg")) {
        resp.setContentType("image/svg+xml");
      } else if (fileName.endsWith(".tree")) {
        resp.setContentType("text/html");
      }
      try (InputStream input = new FileInputStream(requestedFile)) {
        IOUtils.copy(input, resp.getOutputStream());
      }
    }
  }

  private Integer getInteger(final HttpServletRequest req, final String param,
      final Integer defaultValue) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private Long getLong(final HttpServletRequest req, final String param) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Double getMinWidth(final HttpServletRequest req) {
    final String value = req.getParameter("minwidth");
    if (value != null) {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Event getEvent(final HttpServletRequest req) {
    final String eventArg = req.getParameter("event");
    if (eventArg != null) {
      Event event = Event.fromInternalName(eventArg);
      return event == null ? Event.CPU : event;
    }
    return Event.CPU;
  }

  private Output getOutput(final HttpServletRequest req) {
    final String outputArg = req.getParameter("output");
    if (req.getParameter("output") != null) {
      try {
        return Output.valueOf(outputArg.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        return Output.SVG;
      }
    }
    return Output.SVG;
  }

  private void setResponseHeader(final HttpServletResponse response) {
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    response.setContentType(CONTENT_TYPE_TEXT);
  }

  static String getAsyncProfilerHome() {
    String asyncProfilerHome = System.getenv(ASYNC_PROFILER_HOME_ENV);
    // if ENV is not set, see if -Dasync.profiler
    // .home=/path/to/async/profiler/home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      asyncProfilerHome =
          System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
    }

    return asyncProfilerHome;
  }

  enum Event {
    CPU("cpu"),
    ALLOC("alloc"),
    LOCK("lock"),
    PAGE_FAULTS("page-faults"),
    CONTEXT_SWITCHES("context-switches"),
    CYCLES("cycles"),
    INSTRUCTIONS("instructions"),
    CACHE_REFERENCES("cache-references"),
    CACHE_MISSES("cache-misses"),
    BRANCHES("branches"),
    BRANCH_MISSES("branch-misses"),
    BUS_CYCLES("bus-cycles"),
    L1_DCACHE_LOAD_MISSES("L1-dcache-load-misses"),
    LLC_LOAD_MISSES("LLC-load-misses"),
    DTLB_LOAD_MISSES("dTLB-load-misses"),
    MEM_BREAKPOINT("mem:breakpoint"),
    TRACE_TRACEPOINT("trace:tracepoint");

    private String internalName;

    Event(final String internalName) {
      this.internalName = internalName;
    }

    public String getInternalName() {
      return internalName;
    }

    public static Event fromInternalName(final String name) {
      for (Event event : values()) {
        if (event.getInternalName().equalsIgnoreCase(name)) {
          return event;
        }
      }

      return null;
    }
  }

  enum Output {
    SUMMARY,
    TRACES,
    FLAT,
    COLLAPSED,
    SVG,
    TREE,
    JFR
  }

}
