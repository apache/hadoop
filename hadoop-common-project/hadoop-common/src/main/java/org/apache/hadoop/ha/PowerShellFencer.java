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
package org.apache.hadoop.ha;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fencer method that uses PowerShell to remotely connect to a machine and kill
 * the required process. This only works in Windows.
 *
 * The argument passed to this fencer should be a unique string in the
 * "CommandLine" attribute for the "java.exe" process. For example, the full
 * path for the Namenode: "org.apache.hadoop.hdfs.server.namenode.NameNode".
 * The administrator can also shorten the name to "Namenode" if it's unique.
 */
public class PowerShellFencer extends Configured implements FenceMethod {

  private static final Logger LOG = LoggerFactory.getLogger(PowerShellFencer
      .class);


  @Override
  public void checkArgs(String argStr) throws BadFencingConfigurationException {
    LOG.info("The parameter for the PowerShell fencer is " + argStr);
  }

  @Override
  public boolean tryFence(HAServiceTarget target, String argsStr)
      throws BadFencingConfigurationException {

    String processName = argsStr;
    InetSocketAddress serviceAddr = target.getAddress();
    String hostname = serviceAddr.getHostName();

    // Use PowerShell to kill a remote process
    String ps1script = buildPSScript(processName, hostname);
    if (ps1script == null) {
      LOG.error("Cannot build PowerShell script");
      return false;
    }

    // Execute PowerShell script
    LOG.info("Executing " + ps1script);
    ProcessBuilder builder = new ProcessBuilder("powershell.exe", ps1script);
    Process p = null;
    try {
      p = builder.start();
      p.getOutputStream().close();
    } catch (IOException e) {
      LOG.warn("Unable to execute " + ps1script, e);
      return false;
    }

    // Pump logs to stderr
    StreamPumper errPumper = new StreamPumper(
        LOG, "fencer", p.getErrorStream(), StreamPumper.StreamType.STDERR);
    errPumper.start();

    StreamPumper outPumper = new StreamPumper(
        LOG, "fencer", p.getInputStream(), StreamPumper.StreamType.STDOUT);
    outPumper.start();

    // Waiting for the process to finish
    int rc = 0;
    try {
      rc = p.waitFor();
      errPumper.join();
      outPumper.join();
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted while waiting for fencing command: " + ps1script);
      return false;
    }

    return rc == 0;
  }

  /**
   * Build a PowerShell script to kill a java.exe process in a remote machine.
   *
   * @param processName Name of the process to kill. This is an attribute in
   *                    CommandLine.
   * @param host Host where the process is.
   * @return Path of the PowerShell script.
   */
  private String buildPSScript(final String processName, final String host) {
    LOG.info(
        "Building PowerShell script to kill " + processName + " at " + host);
    String ps1script = null;
    BufferedWriter writer = null;
    try {
      File file = File.createTempFile("temp-fence-command", ".ps1");
      file.deleteOnExit();
      FileOutputStream fos = new FileOutputStream(file, false);
      OutputStreamWriter osw =
          new OutputStreamWriter(fos, StandardCharsets.UTF_8);
      writer = new BufferedWriter(osw);

      // Filter to identify the Namenode process
      String filter = StringUtils.join(" and ", new String[] {
          "Name LIKE '%java.exe%'",
          "CommandLine LIKE '%" + processName+ "%'"});

      // Identify the process
      String cmd = "Get-WmiObject Win32_Process";
      cmd += " -Filter \"" + filter + "\"";
      // Remote location
      cmd += " -Computer " + host;
      // Kill it
      cmd += " |% { $_.Terminate() }";

      LOG.info("PowerShell command: " + cmd);
      writer.write(cmd);
      writer.flush();

      ps1script = file.getAbsolutePath();
    } catch (IOException ioe) {
      LOG.error("Cannot create PowerShell script", ioe);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ioe) {
          LOG.error("Cannot close PowerShell script", ioe);
        }
      }
    }
    return ps1script;
  }
}