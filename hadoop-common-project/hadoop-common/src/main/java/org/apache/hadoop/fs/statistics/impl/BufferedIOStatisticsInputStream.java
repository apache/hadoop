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

package org.apache.hadoop.fs.statistics.impl;

import java.io.BufferedInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;

import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;

/**
 * An extension of {@code BufferedInputStream} which implements
 * {@link IOStatisticsSource} and forwards requests for the
 * {@link IOStatistics} to the wrapped stream.
 * This should be used when any input stream needs buffering while
 * allowing the inner stream to be a source of statistics.
 */
public class BufferedIOStatisticsInputStream
    extends BufferedInputStream
    implements IOStatisticsSource {

  public BufferedIOStatisticsInputStream(final InputStream in) {
    super(in);
  }

  public BufferedIOStatisticsInputStream(final InputStream in, final int size) {
    super(in, size);
  }

  @Override
  public IOStatistics getIOStatistics() {
    return retrieveIOStatistics(in);
  }
}
