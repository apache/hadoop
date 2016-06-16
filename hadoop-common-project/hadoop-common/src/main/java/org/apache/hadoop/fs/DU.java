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
package org.apache.hadoop.fs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

/** Filesystem disk space usage statistics.  Uses the unix 'du' program */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DU extends CachingGetSpaceUsed {
  private DUShell duShell;

  @VisibleForTesting
  public DU(File path, long interval, long jitter, long initialUsed)
      throws IOException {
    super(path, interval, jitter, initialUsed);
  }

  public DU(CachingGetSpaceUsed.Builder builder) throws IOException {
    this(builder.getPath(),
        builder.getInterval(),
        builder.getJitter(),
        builder.getInitialUsed());
  }

  @Override
  protected synchronized void refresh() {
    if (duShell == null) {
      duShell = new DUShell();
    }
    try {
      duShell.startRefresh();
    } catch (IOException ioe) {
      LOG.warn("Could not get disk usage information", ioe);
    }
  }

  private final class DUShell extends Shell  {
    void startRefresh() throws IOException {
      run();
    }
    @Override
    public String toString() {
      return
          "du -sk " + getDirPath() + "\n" + used.get() + "\t" + getDirPath();
    }

    @Override
    protected String[] getExecString() {
      return new String[]{"du", "-sk", getDirPath()};
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Expecting a line not the end of stream");
      }
      String[] tokens = line.split("\t");
      if (tokens.length == 0) {
        throw new IOException("Illegal du output");
      }
      setUsed(Long.parseLong(tokens[0]) * 1024);
    }

  }


  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    GetSpaceUsed du = new GetSpaceUsed.Builder().setPath(new File(path))
                                                .setConf(new Configuration())
                                                .build();
    String duResult = du.toString();
    System.out.println(duResult);
  }
}
