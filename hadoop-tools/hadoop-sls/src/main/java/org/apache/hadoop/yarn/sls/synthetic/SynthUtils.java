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
package org.apache.hadoop.yarn.sls.synthetic;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.util.Collection;
import java.util.Random;

/**
 * Utils for the Synthetic generator.
 */
public final class SynthUtils {

  private SynthUtils(){
    //class is not meant to be instantiated
  }

  public static int getWeighted(Collection<Double> weights, Random rr) {

    double totalWeight = 0;
    for (Double i : weights) {
      totalWeight += i;
    }

    double rand = rr.nextDouble() * totalWeight;

    double cur = 0;
    int ind = 0;
    for (Double i : weights) {
      cur += i;
      if (cur > rand) {
        break;
      }
      ind++;
    }

    return ind;
  }

  public static NormalDistribution getNormalDist(JDKRandomGenerator rand,
      double average, double stdDev) {

    if (average <= 0) {
      return null;
    }

    // set default for missing param
    if (stdDev == 0) {
      stdDev = average / 6;
    }

    NormalDistribution ret = new NormalDistribution(average, stdDev,
        NormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
    ret.reseedRandomGenerator(rand.nextLong());
    return ret;
  }

  public static LogNormalDistribution getLogNormalDist(JDKRandomGenerator rand,
      double mean, double stdDev) {

    if (mean <= 0) {
      return null;
    }

    // set default for missing param
    if (stdDev == 0) {
      stdDev = mean / 6;
    }

    // derive lognormal parameters for X = LogNormal(mu, sigma)
    // sigma^2 = ln (1+Var[X]/(E[X])^2)
    // mu = ln(E[X]) - 1/2 * sigma^2
    double var = stdDev * stdDev;
    double sigmasq = Math.log1p(var / (mean * mean));
    double sigma = Math.sqrt(sigmasq);
    double mu = Math.log(mean) - 0.5 * sigmasq;

    LogNormalDistribution ret = new LogNormalDistribution(mu, sigma,
        LogNormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY);
    ret.reseedRandomGenerator(rand.nextLong());
    return ret;
  }
}
