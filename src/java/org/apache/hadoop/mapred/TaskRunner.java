/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.jar.*;
import java.util.logging.*;
import java.util.Vector;
import java.util.Enumeration;

/** Base class that runs a task in a separate process.  Tasks are run in a
 * separate process in order to isolate the map/reduce system code from bugs in
 * user supplied map and reduce functions.
 */
abstract class TaskRunner extends Thread {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.hadoop.mapred.TaskRunner");

  boolean killed = false;
  private Process process;
  private Task t;
  private TaskTracker tracker;

  protected Configuration conf;

  public TaskRunner(Task t, TaskTracker tracker, Configuration conf) {
    this.t = t;
    this.tracker = tracker;
    this.conf = conf;
  }

  public Task getTask() { return t; }
  public TaskTracker getTracker() { return tracker; }

  /** Called to assemble this task's input.  This method is run in the parent
   * process before the child is spawned.  It should not execute user code,
   * only system code. */
  public boolean prepare() throws IOException {return true;}

  /** Called when this task's output is no longer needed.
  * This method is run in the parent process after the child exits.  It should
  * not execute user code, only system code.
  */
  public void close() throws IOException {}

  public final void run() {
    try {

      if (! prepare()) {
        return;
      }

      String sep = System.getProperty("path.separator");
      File workDir = new File(new File(t.getJobFile()).getParent(), "work");
      workDir.mkdirs();
               
      StringBuffer classPath = new StringBuffer();
      // start with same classpath as parent process
      classPath.append(System.getProperty("java.class.path"));
      classPath.append(sep);

      JobConf job = new JobConf(t.getJobFile());
      String jar = job.getJar();
      if (jar != null) {                      // if jar exists, it into workDir
        unJar(new File(jar), workDir);
        File[] libs = new File(workDir, "lib").listFiles();
        if (libs != null) {
          for (int i = 0; i < libs.length; i++) {
            classPath.append(sep);            // add libs from jar to classpath
            classPath.append(libs[i]);
          }
        }
        classPath.append(sep);
        classPath.append(new File(workDir, "classes"));
        classPath.append(sep);
        classPath.append(workDir);
      }

      //  Build exec child jmv args.
      Vector vargs = new Vector(8);
      File jvm =                                  // use same jvm as parent
        new File(new File(System.getProperty("java.home"), "bin"), "java");

      vargs.add(jvm.toString());

      // Add child java ops.  Also, mapred.child.heap.size has been superceded
      // by // mapred.child.java.opts.  Manage case where both are present
      // letting the mapred.child.heap.size win over any setting of heap size in
      // mapred.child.java.opts (Emit a warning that heap.size is deprecated).
      //
      // The following symbols if present in mapred.child.java.opts value are
      // replaced:
      // + @taskid@ is interpolated with value of TaskID.
      // + Replaces @port@ with mapred.task.tracker.report.port + 1.
      // Other occurrences of @ will not be altered.
      //
      // Example with multiple arguments and substitutions, showing
      // jvm GC logging, and start of a passwordless JVM JMX agent so can
      // connect with jconsole and the likes to watch child memory, threads
      // and get thread dumps.
      //
      //     <name>mapred.child.optional.jvm.args</name>
      //     <value>-verbose:gc -Xloggc:/tmp/@taskid@.gc \
      //     -Dcom.sun.management.jmxremote.authenticate=false \
      //     -Dcom.sun.management.jmxremote.ssl=false \
      //     -Dcom.sun.management.jmxremote.port=@port@
      //     </value>
      //
      String javaOpts = handleDeprecatedHeapSize(
          job.get("mapred.child.java.opts", "-Xmx200m"),
          job.get("mapred.child.heap.size"));
      javaOpts = replaceAll(javaOpts, "@taskid@", t.getTaskId());
      int port = job.getInt("mapred.task.tracker.report.port", 50050) + 1;
      javaOpts = replaceAll(javaOpts, "@port@", Integer.toString(port));
      String [] javaOptsSplit = javaOpts.split(" ");
      for (int i = 0; i < javaOptsSplit.length; i++) {
         vargs.add(javaOptsSplit[i]);
      }

      // Add classpath.
      vargs.add("-classpath");
      vargs.add(classPath.toString());
      // Add main class and its arguments 
      vargs.add(TaskTracker.Child.class.getName());  // main of Child
      vargs.add(tracker.taskReportPort + "");        // pass umbilical port
      vargs.add(t.getTaskId());                      // pass task identifier
      // Run java
      runChild((String[])vargs.toArray(new String[0]), workDir);
    } catch (FSError e) {
      LOG.log(Level.SEVERE, "FSError", e);
      try {
        tracker.fsError(e.getMessage());
      } catch (IOException ie) {
        LOG.log(Level.SEVERE, t.getTaskId()+" reporting FSError", ie);
      }
    } catch (Throwable throwable) {
      LOG.log(Level.WARNING, t.getTaskId()+" Child Error", throwable);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      throwable.printStackTrace(new PrintStream(baos));
      try {
        tracker.reportDiagnosticInfo(t.getTaskId(), baos.toString());
      } catch (IOException e) {
        LOG.log(Level.WARNING, t.getTaskId()+" Reporting Diagnostics", e);
      }
    } finally {
      tracker.reportTaskFinished(t.getTaskId());
    }
  }

  /**
   * Handle deprecated mapred.child.heap.size.
   * If present, interpolate into mapred.child.java.opts value with
   * warning.
   * @param javaOpts Value of mapred.child.java.opts property.
   * @param heapSize Value of mapred.child.heap.size property.
   * @return A <code>javaOpts</code> with <code>heapSize</code>
   * interpolated if present.
   */
  private String handleDeprecatedHeapSize(String javaOpts,
          final String heapSize) {
    if (heapSize == null || heapSize.length() <= 0) {
        return javaOpts;
    }
    final String MX = "-Xmx";
    int index = javaOpts.indexOf(MX);
    if (index < 0) {
        javaOpts = javaOpts + " " + MX + heapSize;
    } else {
        int end = javaOpts.indexOf(" ", index + MX.length());
        javaOpts = javaOpts.substring(0, index + MX.length()) +
            heapSize + ((end < 0)? "": javaOpts.substring(end));
    }
    LOG.warning("mapred.child.heap.size is deprecated. Use " +
        "mapred.child.heap.size instead. Meantime, interpolated " +
        "child.heap.size into child.java.opt: " + javaOpts);
    return javaOpts;
  }

  /**
   * Replace <code>toFind</code> with <code>replacement</code>.
   * When hadoop moves to JDK1.5, replace this method with
   * String#replace (Of is commons-lang available, replace with
   * StringUtils#replace). 
   * @param text String to do replacements in.
   * @param toFind String to find.
   * @param replacement String to replace <code>toFind</code> with.
   * @return A String with all instances of <code>toFind</code>
   * replaced by <code>replacement</code> (The original
   * <code>text</code> is returned if <code>toFind</code> is not
   * found in <code>text<code>).
   */
  private static String replaceAll(String text, final String toFind,
      final String replacement) {
    if (text ==  null || toFind ==  null || replacement ==  null) {
      throw new IllegalArgumentException("Text " + text + " or toFind " +
        toFind + " or replacement " + replacement + " are null.");
    }
    int offset = 0;
    for (int index = text.indexOf(toFind); index >= 0;
          index = text.indexOf(toFind, offset)) {
      offset = index + toFind.length();
      text = text.substring(0, index) + replacement +
          text.substring(offset);
        
    }
    return text;
  }

  private void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry)entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            file.getParentFile().mkdirs();
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /**
   * Run the child process
   */
  private void runChild(String[] args, File dir) throws IOException {
    this.process = Runtime.getRuntime().exec(args, null, dir);
    try {
      StringBuffer errorBuf = new StringBuffer();
      new Thread() {
        public void run() {
          logStream(process.getErrorStream());    // copy log output
        }
      }.start();
        
      logStream(process.getInputStream());        // normally empty
      
      if (this.process.waitFor() != 0) {
        throw new IOException("Task process exit with nonzero status.");
      }
      
    } catch (InterruptedException e) {
      throw new IOException(e.toString());
    } finally {
      kill();
    }
  }

  /**
   * Kill the child process
   */
  public void kill() {
      if (process != null) {
          process.destroy();
      }
      killed = true;
  }

  /**
   */
  private void logStream(InputStream output) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(output));
      String line;
      while ((line = in.readLine()) != null) {
        LOG.info(t.getTaskId()+" "+line);
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, t.getTaskId()+" Error reading child output", e);
    } finally {
      try {
        output.close();
      } catch (IOException e) {
        LOG.log(Level.WARNING, t.getTaskId()+" Error closing child output", e);
      }
    }
  }
}
