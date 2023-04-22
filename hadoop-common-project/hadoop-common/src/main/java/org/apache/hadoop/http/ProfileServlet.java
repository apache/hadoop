/*
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

package org.apache.hadoop.http;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.ProcessUtils;

/**
 * Servlet that runs async-profiler as web-endpoint.
 * <p>
 * Following options from async-profiler can be specified as query paramater.
 * //  -e event          profiling event: cpu|alloc|lock|cache-misses etc.
 * //  -d duration       run profiling for 'duration' seconds (integer)
 * //  -i interval       sampling interval in nanoseconds (long)
 * //  -j jstackdepth    maximum Java stack depth (integer)
 * //  -b bufsize        frame buffer size (long)
 * //  -t                profile different threads separately
 * //  -s                simple class names instead of FQN
 * //  -o fmt[,fmt...]   output format: summary|traces|flat|collapsed|svg|tree|jfr|html
 * //  --width px        SVG width pixels (integer)
 * //  --height px       SVG frame height pixels (integer)
 * //  --minwidth px     skip frames smaller than px (double)
 * //  --reverse         generate stack-reversed FlameGraph / Call tree
 * <p>
 * Example:
 * If Namenode http address is localhost:9870, and ResourceManager http address is localhost:8088,
 * ProfileServlet running with async-profiler setup can be accessed with
 * http://localhost:9870/prof and http://localhost:8088/prof for Namenode and ResourceManager
 * processes respectively.
 * Deep dive into some params:
 * - To collect 10 second CPU profile of current process i.e. Namenode (returns FlameGraph svg)
 * curl "http://localhost:9870/prof"
 * - To collect 10 second CPU profile of pid 12345 (returns FlameGraph svg)
 * curl "http://localhost:9870/prof?pid=12345" (For instance, provide pid of Datanode)
 * - To collect 30 second CPU profile of pid 12345 (returns FlameGraph svg)
 * curl "http://localhost:9870/prof?pid=12345&amp;duration=30"
 * - To collect 1 minute CPU profile of current process and output in tree format (html)
 * curl "http://localhost:9870/prof?output=tree&amp;duration=60"
 * - To collect 10 second heap allocation profile of current process (returns FlameGraph svg)
 * curl "http://localhost:9870/prof?event=alloc"
 * - To collect lock contention profile of current process (returns FlameGraph svg)
 * curl "http://localhost:9870/prof?event=lock"
 * <p>
 * Following event types are supported (default is 'cpu') (NOTE: not all OS'es support all events)
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
@InterfaceAudience.Private
public class ProfileServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProfileServlet.class);

  static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String ALLOWED_METHODS = "GET";
  private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
  private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
  private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY = "async.profiler.home";
  private static final String PROFILER_SCRIPT = "/profiler.sh";
  private static final int DEFAULT_DURATION_SECONDS = 10;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);

  static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output-hadoop";

  // This flag is only allowed to be reset by tests.
  private static boolean isTestRun = false;

  private enum Event {

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

    private final String internalName;

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

  private enum Output {
    SUMMARY,
    TRACES,
    FLAT,
    COLLAPSED,
    // No SVG in 2.x asyncprofiler.
    SVG,
    TREE,
    JFR,
    // In 2.x asyncprofiler, this is how you get flamegraphs.
    HTML
  }

  private final Lock profilerLock = new ReentrantLock();
  private transient volatile Process process;
  private final String asyncProfilerHome;
  private Integer pid;

  public ProfileServlet() {
    this.asyncProfilerHome = getAsyncProfilerHome();
    this.pid = ProcessUtils.getPid();
    LOG.info("Servlet process PID: {} asyncProfilerHome: {}", pid, asyncProfilerHome);
  }

  static void setIsTestRun(boolean isTestRun) {
    ProfileServlet.isTestRun = isTestRun;
  }

  @Override
  protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
      throws IOException {
    if (!HttpServer2.isInstrumentationAccessAllowed(getServletContext(), req, resp)) {
      resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      setResponseHeader(resp);
      resp.getWriter().write("Unauthorized: Instrumentation access is not allowed!");
      return;
    }

    // make sure async profiler home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter().write("ASYNC_PROFILER_HOME env is not set.\n\n"
          + "Please ensure the prerequisites for the Profiler Servlet have been installed and the\n"
          + "environment is properly configured.");
      return;
    }

    // if pid is explicitly specified, use it else default to current process
    pid = getInteger(req, "pid", pid);

    // if pid is not specified in query param and if current process pid cannot be determined
    if (pid == null) {
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(resp);
      resp.getWriter().write(
          "'pid' query parameter unspecified or unable to determine PID of current process.");
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
            File outputFile = new File(OUTPUT_DIR,
                "async-prof-pid-" + pid + "-" + event.name().toLowerCase() + "-" + ID_GEN
                    .incrementAndGet() + "." + output.name().toLowerCase());
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
            if (!isTestRun) {
              process = ProcessUtils.runCmdAsync(cmd);
            }

            // set response and set refresh header to output location
            setResponseHeader(resp);
            resp.setStatus(HttpServletResponse.SC_ACCEPTED);
            String relativeUrl = "/prof-output-hadoop/" + outputFile.getName();
            resp.getWriter().write("Started [" + event.getInternalName()
                + "] profiling. This page will automatically redirect to " + relativeUrl + " after "
                + duration + " seconds. "
                + "If empty diagram and Linux 4.6+, see 'Basic Usage' section on the Async "
                + "Profiler Home Page, https://github.com/jvm-profiling-tools/async-profiler."
                + "\n\nCommand:\n" + Joiner.on(" ").join(cmd));

            // to avoid auto-refresh by ProfileOutputServlet, refreshDelay can be specified
            // via url param
            int refreshDelay = getInteger(req, "refreshDelay", 0);

            // instead of sending redirect, set auto-refresh so that browsers will refresh
            // with redirected url
            resp.setHeader("Refresh", (duration + refreshDelay) + ";" + relativeUrl);
            resp.getWriter().flush();
          } finally {
            profilerLock.unlock();
          }
        } else {
          setResponseHeader(resp);
          resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          resp.getWriter()
              .write("Unable to acquire lock. Another instance of profiler might be running.");
          LOG.warn("Unable to acquire lock in {} seconds. Another instance of profiler might be"
              + " running.", lockTimeoutSecs);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while acquiring profile lock.", e);
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } else {
      setResponseHeader(resp);
      resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resp.getWriter().write("Another instance of profiler is already running.");
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
        return Output.HTML;
      }
    }
    return Output.HTML;
  }

  static void setResponseHeader(final HttpServletResponse response) {
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    response.setContentType(CONTENT_TYPE_TEXT);
  }

  static String getAsyncProfilerHome() {
    String asyncProfilerHome = System.getenv(ASYNC_PROFILER_HOME_ENV);
    // if ENV is not set, see if -Dasync.profiler.home=/path/to/async/profiler/home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      asyncProfilerHome = System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
    }

    return asyncProfilerHome;
  }

}
