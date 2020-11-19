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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.TraceUtils;
import org.apache.htrace.core.Tracer;

/**
 * Holds the HTrace Tracer used for FileSystem operations.
 *
 * Ideally, this would be owned by the DFSClient, rather than global.  However,
 * the FileContext API may create a new DFSClient for each operation in some
 * cases.  Because of this, we cannot store this Tracer inside DFSClient.  See
 * HADOOP-6356 for details.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class FsTracer {
  private static Tracer instance;

  public static synchronized Tracer get(Configuration conf) {
    if (instance == null) {
      instance = new Tracer.Builder("FSClient").
          conf(TraceUtils.wrapHadoopConf(CommonConfigurationKeys.
              FS_CLIENT_HTRACE_PREFIX, conf)).
          build();
    }
    return instance;
  }

  @VisibleForTesting
  public static synchronized void clear() {
    if (instance == null) {
      return;
    }
    try {
      instance.close();
    } finally {
      instance = null;
    }
  }

  private FsTracer() {
  }
}
