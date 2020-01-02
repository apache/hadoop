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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the static model for Simple exponential smoothing.
 */
public class SimpleExponentialSmoothing {
  public final static double DEFAULT_FORECAST = -1.0;
  private final int kMinimumReads;
  private final long kStagnatedWindow;
  private final long startTime;
  private long timeConstant;

  private AtomicReference<ForecastRecord> forecastRefEntry;

  public static SimpleExponentialSmoothing createForecast(long timeConstant,
      int skipCnt, long stagnatedWindow, long timeStamp) {
    return new SimpleExponentialSmoothing(timeConstant, skipCnt,
        stagnatedWindow, timeStamp);
  }

  SimpleExponentialSmoothing(long ktConstant, int skipCnt,
      long stagnatedWindow, long timeStamp) {
    kMinimumReads = skipCnt;
    kStagnatedWindow = stagnatedWindow;
    this.timeConstant = ktConstant;
    this.startTime = timeStamp;
    this.forecastRefEntry = new AtomicReference<ForecastRecord>(null);
  }

  private class ForecastRecord {
    private double alpha;
    private long timeStamp;
    private double sample;
    private double rawData;
    private double forecast;
    private double sseError;
    private long myIndex;

    ForecastRecord(double forecast, double rawData, long timeStamp) {
      this(0.0, forecast, rawData, forecast, timeStamp, 0.0, 0);
    }

    ForecastRecord(double alpha, double sample, double rawData,
        double forecast, long timeStamp, double accError, long index) {
      this.timeStamp = timeStamp;
      this.alpha = alpha;
      this.sseError = 0.0;
      this.sample = sample;
      this.forecast = forecast;
      this.rawData = rawData;
      this.sseError = accError;
      this.myIndex = index;
    }

    private double preProcessRawData(double rData, long newTime) {
      return processRawData(this.rawData, this.timeStamp, rData, newTime);
    }

    public ForecastRecord append(long newTimeStamp, double rData) {
      if (this.timeStamp > newTimeStamp) {
        return this;
      }
      double newSample = preProcessRawData(rData, newTimeStamp);
      long deltaTime = this.timeStamp - newTimeStamp;
      if (this.myIndex == kMinimumReads) {
        timeConstant = Math.max(timeConstant, newTimeStamp - startTime);
      }
      double smoothFactor =
          1 - Math.exp(((double) deltaTime) / timeConstant);
      double forecastVal =
          smoothFactor * newSample + (1.0 - smoothFactor) * this.forecast;
      double newSSEError =
          this.sseError + Math.pow(newSample - this.forecast, 2);
      return new ForecastRecord(smoothFactor, newSample, rData, forecastVal,
          newTimeStamp, newSSEError, this.myIndex + 1);
    }

  }

  public boolean isDataStagnated(long timeStamp) {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null && rec.myIndex <= kMinimumReads) {
      return (rec.timeStamp + kStagnatedWindow) < timeStamp;
    }
    return false;
  }

  static double processRawData(double oldRawData, long oldTime,
      double newRawData, long newTime) {
    double rate = (newRawData - oldRawData) / (newTime - oldTime);
    return rate;
  }

  public void incorporateReading(long timeStamp, double rawData) {
    ForecastRecord oldRec = forecastRefEntry.get();
    if (oldRec == null) {
      double oldForecast =
          processRawData(0, startTime, rawData, timeStamp);
      forecastRefEntry.compareAndSet(null,
          new ForecastRecord(oldForecast, 0.0, startTime));
      incorporateReading(timeStamp, rawData);
      return;
    }
    while (!forecastRefEntry.compareAndSet(oldRec, oldRec.append(timeStamp,
        rawData))) {
      oldRec = forecastRefEntry.get();
    }

  }

  public double getForecast() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null && rec.myIndex > kMinimumReads) {
      return rec.forecast;
    }
    return DEFAULT_FORECAST;
  }

  public boolean isDefaultForecast(double value) {
    return value == DEFAULT_FORECAST;
  }

  public double getSSE() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.sseError;
    }
    return DEFAULT_FORECAST;
  }

  public boolean isErrorWithinBound(double bound) {
    double squaredErr = getSSE();
    if (squaredErr < 0) {
      return false;
    }
    return bound > squaredErr;
  }

  public double getRawData() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.rawData;
    }
    return DEFAULT_FORECAST;
  }

  public long getTimeStamp() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.timeStamp;
    }
    return 0L;
  }

  public long getStartTime() {
    return startTime;
  }

  public AtomicReference<ForecastRecord> getForecastRefEntry() {
    return forecastRefEntry;
  }

  @Override
  public String toString() {
    String res = "NULL";
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      res =  "rec.index = " + rec.myIndex + ", forecast t: " + rec.timeStamp +
          ", forecast: " + rec.forecast
          + ", sample: " + rec.sample + ", raw: " + rec.rawData + ", error: "
          + rec.sseError + ", alpha: " + rec.alpha;
    }
    return res;
  }

}
