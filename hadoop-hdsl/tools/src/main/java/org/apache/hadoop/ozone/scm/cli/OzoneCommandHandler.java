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
package org.apache.hadoop.ozone.scm.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.scm.client.ScmClient;

import java.io.IOException;
import java.io.PrintStream;

/**
 * The abstract class of all SCM CLI commands.
 */
public abstract class OzoneCommandHandler {

  private ScmClient scmClient;
  private PrintStream out = System.out;
  private PrintStream err = System.err;

  /**
   * Constructs a handler object.
   */
  public OzoneCommandHandler(ScmClient scmClient) {
    this.scmClient = scmClient;
  }

  protected ScmClient getScmClient() {
    return scmClient;
  }

  /**
   * Sets customized output stream to redirect the stdout to somewhere else.
   * @param out
   */
  public void setOut(PrintStream out) {
    this.out = out;
  }

  /**
   * Sets customized error stream to redirect the stderr to somewhere else.
   * @param err
   */
  public void setErr(PrintStream err) {
    this.err = err;
  }

  public void logOut(String msg, String ... variable) {
    this.out.println(String.format(msg, variable));
  }

  /**
   * Executes the Client command.
   *
   * @param cmd - CommandLine.
   * @throws IOException throws exception.
   */
  public abstract void execute(CommandLine cmd) throws IOException;

  /**
   * Display a help message describing the options the command takes.
   * TODO : currently only prints to standard out, may want to change this.
   */
  public abstract void displayHelp();

  public PrintStream getOut() {
    return out;
  }

  public PrintStream getErr() {
    return err;
  }
}
