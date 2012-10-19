package org.apache.hadoop.cmake.maven.ng;

/*
 * Copyright 2012 The Apache Software Foundation.
 *
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

import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.hadoop.cmake.maven.ng.Utils.OutputToFileThread;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Goal which runs a native unit test.
 *
 * @goal test
 * @phase test
 */
public class TestMojo extends AbstractMojo {
  /**
   * Location of the binary to run.
   *
   * @parameter expression="${binary}"
   * @required
   */
  private File binary;

  /**
   * Name of this test.
   *
   * Defaults to the basename of the binary.  So if your binary is /foo/bar/baz,
   * this will default to 'baz.'
   *
   * @parameter expression="${testName}"
   */
  private String testName;

  /**
   * Environment variables to pass to the binary.
   *
   * @parameter expression="${env}"
   */
  private Map<String, String> env;

  /**
   * Arguments to pass to the binary.
   *
   * @parameter expression="${args}"
   */
  private List<String> args;

  /**
   * Number of seconds to wait before declaring the test failed.
   *
   * @parameter expression="${timeout}" default-value=600
   */
  private int timeout;

  /**
   * Path to results directory.
   *
   * @parameter expression="${results}" default-value="cmake-ng-results"
   */
  private File results;

  /**
   * A list of preconditions which must be true for this test to be run.
   *
   * @parameter expression="${preconditions}"
   */
  private Map<String, String> preconditions = new HashMap<String, String>();

  /**
   * If true, pass over the test without an error if the binary is missing.
   *
   * @parameter expression="${skipIfMissing}" default-value="false"
   */
  private boolean skipIfMissing;
  
  /**
   * What result to expect from the test
   *
   * @parameter expression="${expectedResult}" default-value="success"
   *            Can be either "success", "failure", or "any".
   */
  private String expectedResult;
  
  /**
   * The Maven Session Object
   *
   * @parameter expression="${session}"
   * @required
   * @readonly
   */
  private MavenSession session;
   
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
                testName + ".status"));
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
        getExecutionProperties().getProperty("skipTests");
    if (isTruthy(skipTests)) {
      System.out.println("skipTests is in effect for test " + testName);
      return false;
    }
    // Does the binary exist?  If not, we shouldn't try to run it.
    if (!binary.exists()) {
      if (skipIfMissing) {
        System.out.println("Skipping missing test " + testName);
        return false;
      } else {
        throw new MojoExecutionException("Test " + binary +
            " was not built!  (File does not exist.)");
      }
    }
    // If there is an explicit list of tests to run, it should include this 
    // test.
    String testProp = session.
        getExecutionProperties().getProperty("test");
    if (testProp != null) {
      String testPropArr[] = testProp.split(",");
      boolean found = false;
      for (String test : testPropArr) {
        if (test.equals(testName)) {
          found = true;
          break;
        }
      }
      if (!found) {
        System.out.println("did not find test '" + testName + "' in "
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
        } if (key.equals("and")) {
          if (!isTruthy(val)) {
            System.out.println("Skipping test " + testName +
                " because precondition number " + idx + " was not met.");
            return false;
          }
        } else if (key.equals("andNot")) {
          if (isTruthy(val)) {
            System.out.println("Skipping test " + testName +
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
    Utils.validatePlatform();
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
    StringBuilder stdoutPrefixBuilder = new StringBuilder();
    List<String> cmd = new LinkedList<String>();
    cmd.add(binary.getAbsolutePath());

    System.out.println("-------------------------------------------------------");
    System.out.println(" C M A K E - N G   T E S T");
    System.out.println("-------------------------------------------------------");
    stdoutPrefixBuilder.append("TEST: ").
        append(binary.getAbsolutePath());
    System.out.print(binary.getAbsolutePath());
    for (String entry : args) {
      cmd.add(entry);
      stdoutPrefixBuilder.append(" ").append(entry);
      System.out.print(" ");
      System.out.print(entry);
    }
    System.out.print("\n");
    stdoutPrefixBuilder.append("\n");
    ProcessBuilder pb = new ProcessBuilder(cmd);
    Utils.addEnvironment(pb, env);
    Utils.envronmentToString(stdoutPrefixBuilder, env);
    Process proc = null;
    TestThread testThread = null;
    OutputToFileThread errThread = null, outThread = null;
    int retCode = -1;
    String status = "IN_PROGRESS";
    try {
      writeStatusFile(status);
    } catch (IOException e) {
      throw new MojoExecutionException("Error writing the status file", e);
    }
    try {
      proc = pb.start();
      errThread = new OutputToFileThread(proc.getErrorStream(),
          new File(results, testName + ".stderr"), "");
      errThread.start();
      // Process#getInputStream gets the stdout stream of the process, which 
      // acts as an input to us.
      outThread = new OutputToFileThread(proc.getInputStream(),
          new File(results, testName + ".stdout"),
          stdoutPrefixBuilder.toString());
      outThread.start();
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
          System.err.println("Interrupted while waiting for testThread");
          e.printStackTrace(System.err);
        }
        status = "TIMED_OUT";
      } else if (retCode == 0) {
        status = "SUCCESS";
      } else {
        status = "ERROR " + String.valueOf(retCode);
      }
      try {
        writeStatusFile(status);
      } catch (Exception e) {
        System.err.println("failed to write status file!  Error " + e);
      }
      if (proc != null) {
        proc.destroy();
      }
      // Now that we've terminated the process, the threads servicing
      // its pipes should receive end-of-file and exit.
      // We don't want to terminate them manually or else we might lose
      // some output.
      if (errThread != null) {
        try {
          errThread.interrupt();
          errThread.join();
        } catch (InterruptedException e) {
          System.err.println("Interrupted while waiting for errThread");
          e.printStackTrace(System.err);
        }
        errThread.close();
      }
      if (outThread != null) {
        try {
          outThread.interrupt();
          outThread.join();
        } catch (InterruptedException e) {
          System.err.println("Interrupted while waiting for outThread");
          e.printStackTrace(System.err);
        }
        outThread.close();
      }
    }
    System.out.println("STATUS: " + status);
    System.out.println("-------------------------------------------------------");
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
