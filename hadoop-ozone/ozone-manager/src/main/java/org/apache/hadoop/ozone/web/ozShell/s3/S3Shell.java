/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.ozShell.s3;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.web.ozShell.Shell;
import org.apache.hadoop.ozone.web.ozShell.bucket.S3BucketMapping;
import picocli.CommandLine.Command;

/**
 * Shell for s3 related operations.
 */
@Command(name = "ozone s3",
    description = "Shell for S3 specific operations",
    subcommands = {
        GetS3SecretHandler.class,
        S3BucketMapping.class
    })

public class S3Shell extends Shell {

  @Override
  public void execute(String[] argv) {
    TracingUtil.initTracing("s3shell");
    try (Scope scope = GlobalTracer.get().buildSpan("main").startActive(true)) {
      super.execute(argv);
    }
  }

  /**
   * Main for the S3Shell Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    new S3Shell().run(argv);
  }
}
