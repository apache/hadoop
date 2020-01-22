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

package org.apache.hadoop.fs.impl;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import static java.util.Objects.requireNonNull;

/**
 * All the parameters from the openFile builder for the
 * {@code openFileWithOptions} commands.
 *
 * If/when new attributes added to the builder, this class will be extended.
 */
public class OpenFileParameters {

  /**
   * Set of options declared as mandatory.
   */
  private Set<String> mandatoryKeys;

  /**
   * Options set during the build sequence.
   */
  private Configuration options;

  /**
   * Buffer size.
   */
  private int bufferSize;

  /**
   * Optional file status.
   */
  private FileStatus status;

  public OpenFileParameters() {
  }

  public OpenFileParameters withMandatoryKeys(final Set<String> keys) {
    this.mandatoryKeys = requireNonNull(keys);
    return this;
  }

  public OpenFileParameters withOptions(final Configuration opts) {
    this.options = requireNonNull(opts);
    return this;
  }

  public OpenFileParameters withBufferSize(final int size) {
    this.bufferSize = size;
    return this;
  }

  public OpenFileParameters withStatus(final FileStatus st) {
    this.status = st;
    return this;
  }

  public Set<String> getMandatoryKeys() {
    return mandatoryKeys;
  }

  public Configuration getOptions() {
    return options;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public FileStatus getStatus() {
    return status;
  }
}
