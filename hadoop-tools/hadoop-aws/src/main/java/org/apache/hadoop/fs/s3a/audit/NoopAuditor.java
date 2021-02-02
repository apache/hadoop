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

package org.apache.hadoop.fs.s3a.audit;

import javax.annotation.Nullable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

/**
 * An audit service which returns the {@link NoopSpan}.
 */
public class NoopAuditor extends AbstractOperationAuditor {

  public NoopAuditor(final String name,
      final IOStatisticsStore iostatistics) {
    super(name, iostatistics);
  }

  @Override
  public AuditSpan createSpan(final String name,
      @Nullable final String path1,
      @Nullable final String path2) throws IOException {
    return new NoopSpan(name, path1, path2);
  }

  @Override
  public AuditSpan getUnbondedSpan() {
    return NoopSpan.INSTANCE;
  }

  /**
   * Create, init and start an instance.
   * @param conf configuration.
   * @return a started instance.
   */
  public static OperationAuditor newInstance(Configuration conf) {
    NoopAuditor noop = new NoopAuditor("noop",
        IOStatisticsBinding.emptyStatisticsStore());
    noop.init(conf);
    noop.start();
    return noop;
  }
}
