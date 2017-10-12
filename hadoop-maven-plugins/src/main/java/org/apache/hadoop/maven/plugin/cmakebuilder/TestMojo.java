/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.maven.plugin.cmakebuilder;

import java.util.Locale;
import org.apache.hadoop.maven.plugin.util.Exec;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.TimeUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Goal which runs a native unit test.
 */
@Mojo(name="cmake-test", defaultPhase = LifecyclePhase.TEST)
public class TestMojo extends AbstractMojo {
  /**
   * A value for -Dtest= that runs all native tests.
   */
  private final static String ALL_NATIVE = "allNative";

  /**
   * Location of the binary to run.
   */
  @Parameter(required=true)
  private File binary;

  /**
   * Name of this test.
   *
   * Defaults to the basename of the binary.  So if your binary is /foo/bar/baz,
   * this will default to 'baz.'
   */
  @Parameter
  private String testName;

  /**
   * Environment variables to pass to the binary.
   */
  @Parameter
  private Map<String, String> env;

  /**
   * Arguments to pass to the binary.
   */
  @Parameter
  private List<String> args = new LinkedList<String>();

  /**
   * Number of seconds to wait before declaring the test failed.
   *
   */
  @Parameter(defaultValue="600")
  private int timeout;

  /**
   * The working directory to use.
   */
  @Parameter
  private File workingDirectory;

  /**
   * Path to results directory.
   */
  @Parameter(defaultValue="native-results")
  private File results;

  /**
   * A list of preconditions which must be true for this test to be run.
   */
  @Parameter
  private Map<String, String> preconditions = new HashMap<String, String>();

  /**
   * If true, pass over the test without an error if the binary is missing.
   */
  @Parameter(defaultValue="false")
  private boolean skipIfMissing;

  /**
   * What result to expect from the test
   *
   * Can be either "success", "failure", or "any".
   */
  @Parameter(defaultValue="success")
  private String expectedResult;

  /**
   * The Maven Session Object.
   */
  @Parameter(defaultValue="${session}", readonly=true, required=true)
  private MavenSession session;

  // TODO: support Windows
  private static void validatePlatform() throws MojoExecutionException {
    if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH)
        .startsWith("windows")) {
      throw new MojoExecutionException("CMakeBuilder does not yet support " +
          "the Windows platform.");
    }
  }

  /**
   * The test thread waits for the process to terminate.
   *
   * Since Process#waitFor doesn't take a timeout argument, we simulate one by
   * interrupting this thread after a certain amount of time has elapsed.
   */
  private static class TestThread extends Thread {
    private Process proc;
    private int retCode = -1;

    public TestThread(Process proc) {
      this.proc = proc;
    }

    public void run() {
      try {
        retCode = proc.waitFor();
      } catch (InterruptedException e) {
        retCode = -1;
      }
    }

    public int retCode() {
      return retCode;
    }
  }

  /**
   * Write to the status file.
   *
   * The status file will contain a string describing the exit status of the
   * test.  It will be SUCCESS if the test returned success (return code 0), a
   * numerical code if it returned a non-zero status, or IN_PROGRESS or
   * TIMED_OUT.
   */
  private void writeStatusFile(String status) throws IOException {
    FileOutputStream fos = new FileOutputStream(new File(results,
                testName + ".pstatus"));
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(fos, "UTF8"));
      out.write(status + "\n");
    } finally {
      if (out != null) {
        out.close();
      } else {
        fos.close();
      }
    }
  }

  private static boolean isTruthy(String str) {
    if (str == null)
      return false;
    if (str.equalsIgnoreCase(""))
      return false;
    if (str.equalsIgnoreCase("false"))
      return false;
    if (str.equalsIgnoreCase("no"))
      return false;
    if (str.equalsIgnoreCase("off"))
      return false;
    if (str.equalsIgnoreCase("disable"))
      return false;
    return true;
  }


  final static private String VALID_PRECONDITION_TYPES_STR =
      "Valid precondition types are \"and\", \"andNot\"";

  /**
   * Validate the parameters that the user has passed.
   * @throws MojoExecutionException
   */
  private void validateParameters() throws MojoExecutionException {
    if (!(expectedResult.equals("success") ||
        expectedResult.equals("failure") ||
        expectedResult.equals("any"))) {
      throw new MojoExecutionException("expectedResult must be either " +
          "success, failure, or any");
    }
  }

  private boolean shouldRunTest() throws MojoExecutionException {
    // Were we told to skip all tests?
    String skipTests = session.
        getSystemProperties().getProperty("skipTests");
    if (isTruthy(skipTests)) {
      getLog().info("skipTests is in effect for test " + testName);
      return false;
    }
    // Does the binary exist?  If not, we shouldn't try to run it.
    if (!binary.exists()) {
      if (skipIfMissing) {
        getLog().info("Skipping missing test " + testName);
        return false;
      } else {
        throw new MojoExecutionException("Test " + binary +
            " was not built!  (File does not exist.)");
      }
    }
    // If there is an explicit list of tests to run, it should include this
    // test.
    String testProp = session.
        getSystemProperties().getProperty("test");
    if (testProp != null) {
      String testPropArr[] = testProp.split(",");
      boolean found = false;
      for (String test : testPropArr) {
        if (test.equals(ALL_NATIVE)) {
          found = true;
          break;
        }
        if (test.equals(testName)) {
          found = true;
          break;
        }
      }
      if (!found) {
        getLog().debug("did not find test '" + testName + "' in "
             + "list " + testProp);
        return false;
      }
    }
    // Are all the preconditions satistfied?
    if (preconditions != null) {
      int idx = 1;
      for (Map.Entry<String, String> entry : preconditions.entrySet()) {
        String key = entry.getKey();
        String val = entry.getValue();
        if (key == null) {
          throw new MojoExecutionException("NULL is not a valid " +
               "precondition type.  " + VALID_PRECONDITION_TYPES_STR);
        }
        if (key.equals("and")) {
          if (!isTruthy(val)) {
            getLog().info("Skipping test " + testName +
                " because precondition number " + idx + " was not met.");
            return false;
          }
        } else if (key.equals("andNot")) {
          if (isTruthy(val)) {
            getLog().info("Skipping test " + testName +
                " because negative precondition number " + idx +
                " was met.");
            return false;
          }
        } else {
          throw new MojoExecutionException(key + " is not a valid " +
              "precondition type.  " + VALID_PRECONDITION_TYPES_STR);
        }
        idx++;
      }
    }
    // OK, we should run this.
    return true;
  }

  public void execute() throws MojoExecutionException {
    if (testName == null) {
      testName = binary.getName();
    }
    validatePlatform();
    validateParameters();
    if (!shouldRunTest()) {
      return;
    }
    if (!results.isDirectory()) {
      if (!results.mkdirs()) {
        throw new MojoExecutionException("Failed to create " +
            "output directory '" + results + "'!");
      }
    }
    List<String> cmd = new LinkedList<String>();
    cmd.add(binary.getAbsolutePath());

    getLog().info("-------------------------------------------------------");
    getLog().info(" C M A K E B U I L D E R    T E S T");
    getLog().info("-------------------------------------------------------");
    StringBuilder bld = new StringBuilder();
    bld.append(testName).append(": running ");
    bld.append(binary.getAbsolutePath());
    for (String entry : args) {
      cmd.add(entry);
      bld.append(" ").append(entry);
    }
    getLog().info(bld.toString());
    ProcessBuilder pb = new ProcessBuilder(cmd);
    Exec.addEnvironment(pb, env);
    if (workingDirectory != null) {
      pb.directory(workingDirectory);
    }
    pb.redirectError(new File(results, testName + ".stderr"));
    pb.redirectOutput(new File(results, testName + ".stdout"));
    getLog().info("with extra environment variables " + Exec.envToString(env));
    Process proc = null;
    TestThread testThread = null;
    int retCode = -1;
    String status = "IN_PROGRESS";
    try {
      writeStatusFile(status);
    } catch (IOException e) {
      throw new MojoExecutionException("Error writing the status file", e);
    }
    long start = System.nanoTime();
    try {
      proc = pb.start();
      testThread = new TestThread(proc);
      testThread.start();
      testThread.join(timeout * 1000);
      if (!testThread.isAlive()) {
        retCode = testThread.retCode();
        testThread = null;
        proc = null;
      }
    } catch (IOException e) {
      throw new MojoExecutionException("IOException while executing the test " +
          testName, e);
    } catch (InterruptedException e) {
      throw new MojoExecutionException("Interrupted while executing " +
          "the test " + testName, e);
    } finally {
      if (testThread != null) {
        // If the test thread didn't exit yet, that means the timeout expired.
        testThread.interrupt();
        try {
          testThread.join();
        } catch (InterruptedException e) {
          getLog().error("Interrupted while waiting for testThread", e);
        }
        status = "TIMED OUT";
      } else if (retCode == 0) {
        status = "SUCCESS";
      } else {
        status = "ERROR CODE " + String.valueOf(retCode);
      }
      try {
        writeStatusFile(status);
      } catch (Exception e) {
        getLog().error("failed to write status file!", e);
      }
      if (proc != null) {
        proc.destroy();
      }
    }
    long end = System.nanoTime();
    getLog().info("STATUS: " + status + " after " +
          TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS) +
          " millisecond(s).");
    getLog().info("-------------------------------------------------------");
    if (status.equals("TIMED_OUT")) {
      if (expectedResult.equals("success")) {
        throw new MojoExecutionException("Test " + binary +
            " timed out after " + timeout + " seconds!");
      }
    } else if (!status.equals("SUCCESS")) {
      if (expectedResult.equals("success")) {
        throw new MojoExecutionException("Test " + binary +
            " returned " + status);
      }
    } else if (expectedResult.equals("failure")) {
      throw new MojoExecutionException("Test " + binary +
          " succeeded, but we expected failure!");
    }
  }
}
