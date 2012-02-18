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

package org.apache.hadoop.fs.slive;

import org.apache.hadoop.fs.slive.WeightSelector.Weightable;

/**
 * Class to isolate the various weight algorithms we use.
 */
class Weights {
  private Weights() {

  }

  /**
   * A weight which always returns the same weight (1/3). Which will have an
   * overall area of (1/3) unless otherwise provided.
   */
  static class UniformWeight implements Weightable {

    private static Double DEFAULT_WEIGHT = (1.0d / 3.0d);

    private Double weight;

    UniformWeight(double w) {
      weight = w;
    }

    UniformWeight() {
      this(DEFAULT_WEIGHT);
    }

    @Override // Weightable
    public Double weight(int elapsed, int duration) {
      return weight;
    }

  }

  /**
   * A weight which normalized the elapsed time and the duration to a value
   * between 0 and 1 and applies the algorithm to form an output using the
   * function (-2 * (x-0.5)^2) + 0.5 which initially (close to 0) has a value
   * close to 0 and near input being 1 has a value close to 0 and near 0.5 has a
   * value close to 0.5 (with overall area 0.3).
   */
  static class MidWeight implements Weightable {

    @Override // Weightable
    public Double weight(int elapsed, int duration) {
      double normalized = (double) elapsed / (double) duration;
      double result = (-2.0d * Math.pow(normalized - 0.5, 2)) + 0.5d;
      if (result < 0) {
        result = 0;
      }
      if (result > 1) {
        result = 1;
      }
      return result;
    }

  }

  /**
   * A weight which normalized the elapsed time and the duration to a value
   * between 0 and 1 and applies the algorithm to form an output using the
   * function (x)^2 which initially (close to 0) has a value close to 0 and near
   * input being 1 has a value close to 1 (with overall area 1/3).
   */
  static class EndWeight implements Weightable {

    @Override // Weightable
    public Double weight(int elapsed, int duration) {
      double normalized = (double) elapsed / (double) duration;
      double result = Math.pow(normalized, 2);
      if (result < 0) {
        result = 0;
      }
      if (result > 1) {
        result = 1;
      }
      return result;
    }

  }

  /**
   * A weight which normalized the elapsed time and the duration to a value
   * between 0 and 1 and applies the algorithm to form an output using the
   * function (x-1)^2 which initially (close to 0) has a value close to 1 and
   * near input being 1 has a value close to 0 (with overall area 1/3).
   */
  static class BeginWeight implements Weightable {

    @Override // Weightable
    public Double weight(int elapsed, int duration) {
      double normalized = (double) elapsed / (double) duration;
      double result = Math.pow((normalized - 1), 2);
      if (result < 0) {
        result = 0;
      }
      if (result > 1) {
        result = 1;
      }
      return result;
    }

  }

}
