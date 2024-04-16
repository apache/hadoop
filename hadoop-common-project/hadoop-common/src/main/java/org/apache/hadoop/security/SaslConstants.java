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
package org.apache.hadoop.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL related constants.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslConstants {
  public static final Logger LOG = LoggerFactory.getLogger(SaslConstants.class);

  private static final String SASL_MECHANISM_ENV = "HADOOP_SASL_MECHANISM";
  public static final String SASL_MECHANISM;
  private static final String SASL_MECHANISM_DEFAULT = "DIGEST-MD5";

  static {
    final String mechanism = System.getenv(SASL_MECHANISM_ENV);
    LOG.debug("{} = {} (env)", SASL_MECHANISM_ENV, mechanism);
    SASL_MECHANISM = mechanism != null? mechanism : SASL_MECHANISM_DEFAULT;
    LOG.debug("{} = {} (effective)", SASL_MECHANISM_ENV, SASL_MECHANISM);
  }
}