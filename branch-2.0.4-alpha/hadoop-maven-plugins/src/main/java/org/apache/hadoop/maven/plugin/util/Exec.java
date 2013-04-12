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
package org.apache.hadoop.maven.plugin.util;

import org.apache.maven.plugin.Mojo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Exec is a helper class for executing an external process from a mojo.
 */
public class Exec {
  private Mojo mojo;

  /**
   * Creates a new Exec instance for executing an external process from the given
   * mojo.
   * 
   * @param mojo Mojo executing external process
   */
  public Exec(Mojo mojo) {
    this.mojo = mojo;
  }

  /**
   * Runs the specified command and saves each line of the command's output to
   * the given list.
   * 
   * @param command List<String> containing command and all arguments
   * @param output List<String> in/out parameter to receive command output
   * @return int exit code of command
   */
  public int run(List<String> command, List<String> output) {
    int retCode = 1;
    ProcessBuilder pb = new ProcessBuilder(command);
    try {
      Process p = pb.start();
      OutputBufferThread stdOut = new OutputBufferThread(p.getInputStream());
      OutputBufferThread stdErr = new OutputBufferThread(p.getErrorStream());
      stdOut.start();
      stdErr.start();
      retCode = p.waitFor();
      if (retCode != 0) {
        mojo.getLog().warn(command + " failed with error code " + retCode);
        for (String s : stdErr.getOutput()) {
          mojo.getLog().debug(s);
        }
      } else {
        stdOut.join();
        stdErr.join();
        output.addAll(stdOut.getOutput());
      }
    } catch (Exception ex) {
      mojo.getLog().warn(command + " failed: " + ex.toString());
    }
    return retCode;
  }

  /**
   * OutputBufferThread is a background thread for consuming and storing output
   * of the external process.
   */
  private static class OutputBufferThread extends Thread {
    private List<String> output;
    private BufferedReader reader;

    /**
     * Creates a new OutputBufferThread to consume the given InputStream.
     * 
     * @param is InputStream to consume
     */
    public OutputBufferThread(InputStream is) {
      this.setDaemon(true);
      output = new ArrayList<String>();
      reader = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public void run() {
      try {
        String line = reader.readLine();
        while (line != null) {
          output.add(line);
          line = reader.readLine();
        }
      } catch (IOException ex) {
        throw new RuntimeException("make failed with error code " + ex.toString());
      }
    }

    /**
     * Returns every line consumed from the input.
     * 
     * @return List<String> every line consumed from the input
     */
    public List<String> getOutput() {
      return output;
    }
  }
}
