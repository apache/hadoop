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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Context data for an ongoing NameNode metadata recovery process. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MetaRecoveryContext  {
  public static final Logger LOG =
      LoggerFactory.getLogger(MetaRecoveryContext.class.getName());
  public final static int FORCE_NONE = 0;
  public final static int FORCE_FIRST_CHOICE = 1;
  public final static int FORCE_ALL = 2;
  private int force;
  
  /** Exception thrown when the user has requested processing to stop. */
  static public class RequestStopException extends IOException {
    private static final long serialVersionUID = 1L;
    public RequestStopException(String msg) {
      super(msg);
    }
  }
  
  public MetaRecoveryContext(int force) {
    this.force = force;
  }

  /**
   * Display a prompt to the user and get his or her choice.
   *  
   * @param prompt      The prompt to display
   * @param firstChoice First choice (will be taken if autoChooseDefault is
   *                    true)
   * @param choices     Other choies
   *
   * @return            The choice that was taken
   * @throws IOException
   */
  public String ask(String prompt, String firstChoice, String... choices) 
      throws IOException {
    while (true) {
      System.err.print(prompt);
      if (force > FORCE_NONE) {
        System.out.println("automatically choosing " + firstChoice);
        return firstChoice;
      }
      StringBuilder responseBuilder = new StringBuilder();
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
      String response = responseBuilder.toString();
      if (response.equalsIgnoreCase(firstChoice))
        return firstChoice;
      for (String c : choices) {
        if (response.equalsIgnoreCase(c)) {
          return c;
        }
      }
      System.err.print("I'm sorry, I cannot understand your response.\n");
    }
  }

  public static void editLogLoaderPrompt(String prompt,
        MetaRecoveryContext recovery, String contStr)
        throws IOException, RequestStopException
  {
    if (recovery == null) {
      throw new IOException(prompt);
    }
    LOG.error(prompt);
    String answer = recovery.ask("\nEnter 'c' to continue, " + contStr + "\n" +
      "Enter 's' to stop reading the edit log here, abandoning any later " +
        "edits\n" +
      "Enter 'q' to quit without saving\n" +
      "Enter 'a' to always select the first choice in the future " +
      "without prompting. " + 
      "(c/s/q/a)\n", "c", "s", "q", "a");
    if (answer.equals("c")) {
      LOG.info("Continuing");
      return;
    } else if (answer.equals("s")) {
      throw new RequestStopException("user requested stop");
    } else if (answer.equals("q")) {
      recovery.quit();
    } else {
      recovery.setForce(FORCE_FIRST_CHOICE);
      return;
    }
  }

  /** Log a message and quit */
  public void quit() {
    LOG.error("Exiting on user request.");
    System.exit(0);
  }

  public int getForce() {
    return this.force;
  }

  public void setForce(int force) {
    this.force = force;
  }
}
