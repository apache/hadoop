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
package org.apache.hadoop.examples.pi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.examples.pi.math.Summation;
import org.apache.hadoop.io.Writable;

/** A class for map task results or reduce task results. */
public class TaskResult implements Container<Summation>,
    Combinable<TaskResult>, Writable {
  private Summation sigma;
  private long duration;

  public TaskResult() {}

  TaskResult(Summation sigma, long duration) {
    this.sigma = sigma;
    this.duration = duration;      
  }

  /** {@inheritDoc} */
  @Override
  public Summation getElement() {
    return sigma;
  }

  /** @return The time duration used */
  long getDuration() {
    return duration;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(TaskResult that) {
    return this.sigma.compareTo(that.sigma);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    else if (obj instanceof TaskResult) {
      final TaskResult that = (TaskResult)obj;
      return this.compareTo(that) == 0;
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported. */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** {@inheritDoc} */
  @Override
  public TaskResult combine(TaskResult that) {
    final Summation s = sigma.combine(that.sigma);
    return s == null ? null : new TaskResult(s, this.duration + that.duration);
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    sigma = SummationWritable.read(in);
    duration = in.readLong();
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    SummationWritable.write(sigma, out);
    out.writeLong(duration);
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "sigma=" + sigma + ", duration=" + duration + "(" +
        Util.millis2String(duration) + ")";
  }

  /** Covert a String to a TaskResult. */
  public static TaskResult valueOf(String s) {
    int i = 0;
    int j = s.indexOf(", duration=");
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j +
          " < 0, s=" + s);
    final Summation sigma =
        Summation.valueOf(Util.parseStringVariable("sigma", s.substring(i, j)));

    i = j + 2;
    j = s.indexOf("(", i);
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j +
          " < 0, s=" + s);
    final long duration =
        Util.parseLongVariable("duration", s.substring(i, j));

    return new TaskResult(sigma, duration);
  }
}