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

package org.apache.hadoop.mapreduce.v2.app.speculate.forecast;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.junit.Assert;
import org.junit.Test;

/**
 * Testing the statistical model of simple exponential estimator.
 */
public class TestSimpleExponentialForecast {
  private static final Log LOG =
      LogFactory.getLog(TestSimpleExponentialForecast.class);

  private static long clockTicks = 1000L;
  private ControlledClock clock;

  private int  incTestSimpleExponentialForecast() {
    clock = new ControlledClock();
    clock.tickMsec(clockTicks);
    SimpleExponentialSmoothing forecaster =
        new SimpleExponentialSmoothing(10000,
            12, 10000, clock.getTime());


    double progress = 0.0;

    while(progress <= 1.0) {
      clock.tickMsec(clockTicks);
      forecaster.incorporateReading(clock.getTime(), progress);
      LOG.info("progress: " + progress + " --> " + forecaster.toString());
      progress += 0.005;
    }

    return forecaster.getSSE() < Math.pow(10.0, -6) ? 0 : 1;
  }


  private int decTestSimpleExponentialForecast() {
    clock = new ControlledClock();
    clock.tickMsec(clockTicks);
    SimpleExponentialSmoothing forecaster =
        new SimpleExponentialSmoothing(800,
            12, 10000, clock.getTime());

    double progress = 0.0;

    double[] progressRates = new double[]{0.005, 0.004, 0.002, 0.001};
    while(progress <= 1.0) {
      clock.tickMsec(clockTicks);
      forecaster.incorporateReading(clock.getTime(), progress);
      LOG.info("progress: " + progress + " --> " + forecaster.toString());
      progress += progressRates[(int)(progress / 0.25)];
    }

    return forecaster.getSSE() < Math.pow(10.0, -6) ? 0 : 1;
  }

  private int zeroTestSimpleExponentialForecast() {
    clock = new ControlledClock();
    clock.tickMsec(clockTicks);
    SimpleExponentialSmoothing forecaster =
        new SimpleExponentialSmoothing(800,
            12, 10000, clock.getTime());

    double progress = 0.0;

    double[] progressRates = new double[]{0.005, 0.004, 0.002, 0.0, 0.003};
    int progressInd = 0;
    while(progress <= 1.0) {
      clock.tickMsec(clockTicks);
      forecaster.incorporateReading(clock.getTime(), progress);
      LOG.info("progress: " + progress + " --> " + forecaster.toString());
      int currInd = progressInd++ > 1000 ? 4 : (int)(progress / 0.25);
      progress += progressRates[currInd];
    }

    return forecaster.getSSE() < Math.pow(10.0, -6) ? 0 : 1;
  }

  @Test
  public void testSimpleExponentialForecastLinearInc() throws Exception {
    int res = incTestSimpleExponentialForecast();
    Assert.assertEquals("We got the wrong estimate from simple exponential.",
        res, 0);
  }

  @Test
  public void testSimpleExponentialForecastLinearDec() throws Exception {
    int res = decTestSimpleExponentialForecast();
    Assert.assertEquals("We got the wrong estimate from simple exponential.",
        res, 0);
  }

  @Test
  public void testSimpleExponentialForecastZeros() throws Exception {
    int res = zeroTestSimpleExponentialForecast();
    Assert.assertEquals("We got the wrong estimate from simple exponential.",
        res, 0);
  }
}
