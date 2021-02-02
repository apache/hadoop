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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.service.AbstractService;

/**
 * This is a long-lived service which is created in S3A FS initialize
 * (make it fast!) which provides context for tracking operations made to S3.
 * An IOStatisticsStore is passed in -in production this is expected to
 * be the S3AFileSystem instrumentation, which will have the
 * {@code AUDIT_SPAN_START} statistic configured for counting durations.
 */
public abstract class AbstractOperationAuditor extends AbstractService
    implements OperationAuditor {

  /**
   * Destination for recording statistics, especially duration/count of
   * operations.
   */
  private final IOStatisticsStore iostatistics;

  /**
   * Construct.
   * @param name name
   * @param iostatistics store of statistics.
   */
  protected AbstractOperationAuditor(final String name,
      final IOStatisticsStore iostatistics) {
    super(name);
    this.iostatistics = iostatistics;
  }

  /**
   * Get the IOStatistics Store.
   * @return the IOStatistics store updated with statistics.
   */
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * Create, and initialize and an audit service.
   * The service start operation is not called: that is left to
   * the caller.
   * @param conf configuration to read the key from and to use to init
   * the service.
   * @param key key containing the classname
   * @param store IOStatistics store.
   * @return instantiated class.
   * @throws IOException failure to initialise.
   */
  @SuppressWarnings("ClassReferencesSubclass")
  public static OperationAuditor createInstance(
      Configuration conf,
      String key,
      IOStatisticsStore store) throws IOException {
    try {
      final Class<? extends OperationAuditor> auditClassname
          = conf.getClass(
          key,
          LoggingAuditor.class,
          OperationAuditor.class);
      final Constructor<? extends OperationAuditor> constructor
          = auditClassname.getConstructor(String.class,
          IOStatisticsStore.class);
      final OperationAuditor instance = constructor.newInstance(
          auditClassname.getCanonicalName(), store);
      instance.init(conf);
      return instance;
    } catch (NoSuchMethodException | InstantiationException
        | RuntimeException
        | IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Failed to instantiate class "
          + "defined in " + key,
          e);
    }
  }

}
