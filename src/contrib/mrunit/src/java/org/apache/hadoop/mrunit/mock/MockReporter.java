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
package org.apache.hadoop.mrunit.mock;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

public class MockReporter implements Reporter {

  private MockInputSplit inputSplit = new MockInputSplit();

  public enum ReporterType {
    Mapper,
    Reducer
  }

  private ReporterType typ;

  public MockReporter(final ReporterType kind) {
    this.typ = kind;
  }

  @Override
  public InputSplit getInputSplit() {
    if (typ == ReporterType.Reducer) {
      throw new UnsupportedOperationException(
              "Reducer cannot call getInputSplit()");
    } else {
      return inputSplit;
    }
  }

  @Override
  public void incrCounter(Enum key, long amount) {
    // do nothing.
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    // do nothing.
  }

  @Override
  public void setStatus(String status) {
    // do nothing.
  }

  @Override
  public void progress() {
    // do nothing.
  }

  @Override
  public Counter getCounter(String s1, String s2) {
    // do nothing
    return null;
  }

  @Override
  public Counter getCounter(Enum key) {
    // do nothing
    return null;
  }
}

