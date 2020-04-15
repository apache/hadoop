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

package org.apache.hadoop.fs.s3a.tools;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.util.ExitUtil;

/**
 * Handle directory-related command-line options in the
 * s3guard tool.
 * <pre>
 *   scan: scan for markers
 *   clean: clean up marker entries. This will include empty directories
 *   created with -mkdir
 * </pre>
 * This tool does not go anywhere near S3Guard.
 */
public class MarkerTool extends S3GuardTool {

  private static final Logger LOG =
      LoggerFactory.getLogger(MarkerTool.class);

  public static final String NAME = "markers";

  public static final String PURPOSE =
      "view and manipulate S3 directory markers";

  private static final String USAGE = NAME
      + " [OPTIONS]"
      + " [-scan]"
      + " [-clean]"
      + " [-out <path>]"
      + " [-" + VERBOSE + "]"
      + " <PATH>\n"
      + "\t" + PURPOSE + "\n\n";

  public static final String OPT_EXPECTED = "expected";

  public static final String OPT_OUTPUT = "out";

  public MarkerTool(final Configuration conf,
      final String... opts) {
    super(conf, opts);
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int run(final String[] args, final PrintStream out)
      throws Exception, ExitUtil.ExitException {
    return 0;
  }

  static interface MarkerOperationCallbacks {

  }

  /**
   * Maintenance operations isolated from command line for ease of testing.
   */
  static final class MarkerMaintenance {
    private final StoreContext store;
    private final MarkerOperationCallbacks operationCallbacks;

    MarkerMaintenance(final StoreContext store,
        final MarkerOperationCallbacks operationCallbacks) {
      this.store = store;
      this.operationCallbacks = operationCallbacks;
    }

  }
}
