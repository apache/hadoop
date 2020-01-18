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
  private static final double DEFAULT_FORECAST = -1.0;
  private final int kMinimumReads;
  private final long kStagnatedWindow;
  private final long startTime;
  private long timeConstant;

  /**
   * Holds reference to the current forecast record.
   */
  private AtomicReference<ForecastRecord> forecastRefEntry;

  public static SimpleExponentialSmoothing createForecast(
      final long timeConstant,
      final int skipCnt, final long stagnatedWindow, final long timeStamp) {
    return new SimpleExponentialSmoothing(timeConstant, skipCnt,
        stagnatedWindow, timeStamp);
  }

  SimpleExponentialSmoothing(final long ktConstant, final int skipCnt,
      final long stagnatedWindow, final long timeStamp) {
    this.kMinimumReads = skipCnt;
    this.kStagnatedWindow = stagnatedWindow;
    this.timeConstant = ktConstant;
    this.startTime = timeStamp;
    this.forecastRefEntry = new AtomicReference<ForecastRecord>(null);
  }

  private class ForecastRecord {
    private final double alpha;
    private final long timeStamp;
    private final double sample;
    private final double rawData;
    private double forecast;
    private final double sseError;
    private final long myIndex;
    private ForecastRecord prevRec;

    ForecastRecord(final double currForecast, final double currRawData,
        final long currTimeStamp) {
      this(0.0, currForecast, currRawData, currForecast, currTimeStamp, 0.0, 0);
    }

    ForecastRecord(final double alphaVal, final double currSample,
        final double currRawData,
        final double currForecast, final long currTimeStamp,
        final double accError,
        final long index) {
      this.timeStamp = currTimeStamp;
      this.alpha = alphaVal;
      this.sample = currSample;
      this.forecast = currForecast;
      this.rawData = currRawData;
      this.sseError = accError;
      this.myIndex = index;
    }

    private ForecastRecord createForecastRecord(final double alphaVal,
        final double currSample,
        final double currRawData,
        final double currForecast, final long currTimeStamp,
        final double accError,
        final long index,
        final ForecastRecord prev) {
      ForecastRecord forecastRec =
          new ForecastRecord(alphaVal, currSample, currRawData, currForecast,
              currTimeStamp, accError, index);
      forecastRec.prevRec = prev;
      return forecastRec;
    }

    private double preProcessRawData(final double rData, final long newTime) {
      return processRawData(this.rawData, this.timeStamp, rData, newTime);
    }

    public ForecastRecord append(final long newTimeStamp, final double rData) {
      if (this.timeStamp >= newTimeStamp
          && Double.compare(this.rawData, rData) >= 0) {
        // progress reported twice. Do nothing.
        return this;
      }
      ForecastRecord refRecord = this;
      if (newTimeStamp == this.timeStamp) {
        // we need to restore old value if possible
        if (this.prevRec != null) {
          refRecord = this.prevRec;
        }
      }
      double newSample = refRecord.preProcessRawData(rData, newTimeStamp);
      long deltaTime = this.timeStamp - newTimeStamp;
      if (refRecord.myIndex == kMinimumReads) {
        timeConstant = Math.max(timeConstant, newTimeStamp - startTime);
      }
      double smoothFactor =
          1 - Math.exp(((double) deltaTime) / timeConstant);
      double forecastVal =
          smoothFactor * newSample + (1.0 - smoothFactor) * refRecord.forecast;
      double newSSEError =
          refRecord.sseError + Math.pow(newSample - refRecord.forecast, 2);
      return refRecord
          .createForecastRecord(smoothFactor, newSample, rData, forecastVal,
              newTimeStamp, newSSEError, refRecord.myIndex + 1, refRecord);
    }
  }

  /**
   * checks if the task is hanging up.
   * @param timeStamp current time of the scan.
   * @return true if we have number of samples {@literal >} kMinimumReads and the
   * record timestamp has expired.
   */
  public boolean isDataStagnated(final long timeStamp) {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null && rec.myIndex > kMinimumReads) {
      return (rec.timeStamp + kStagnatedWindow) > timeStamp;
    }
    return false;
  }

  static double processRawData(final double oldRawData, final long oldTime,
      final double newRawData, final long newTime) {
    double rate = (newRawData - oldRawData) / (newTime - oldTime);
    return rate;
  }

  public void incorporateReading(final long timeStamp,
      final double currRawData) {
    ForecastRecord oldRec = forecastRefEntry.get();
    if (oldRec == null) {
      double oldForecast =
          processRawData(0, startTime, currRawData, timeStamp);
      forecastRefEntry.compareAndSet(null,
          new ForecastRecord(oldForecast, 0.0, startTime));
      incorporateReading(timeStamp, currRawData);
      return;
    }
    while (!forecastRefEntry.compareAndSet(oldRec, oldRec.append(timeStamp,
        currRawData))) {
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

  public boolean isDefaultForecast(final double value) {
    return value == DEFAULT_FORECAST;
  }

  public double getSSE() {
    ForecastRecord rec = forecastRefEntry.get();
    if (rec != null) {
      return rec.sseError;
    }
    return DEFAULT_FORECAST;
  }

  public boolean isErrorWithinBound(final double bound) {
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
      res =  "rec.index = " + rec.myIndex + ", forecast t: " + rec.timeStamp
          + ", forecast: " + rec.forecast
          + ", sample: " + rec.sample + ", raw: " + rec.rawData + ", error: "
          + rec.sseError + ", alpha: " + rec.alpha;
    }
    return res;
  }

}
