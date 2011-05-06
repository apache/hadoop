/*
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.AggregateProtocol;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This client class is for invoking the aggregate functions deployed on the
 * Region Server side via the AggregateProtocol. This class will implement the
 * supporting functionality for summing/processing the individual results
 * obtained from the AggregateProtocol for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate
 * functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the
 * parameter type. For average and std, it returns a double value. For row
 * count, it returns a long value.
 */
public class AggregationClient {

  private static final Log log = LogFactory.getLog(AggregationClient.class);
  Configuration conf;

  /**
   * Constructor with Conf object
   * @param cfg
   */
  public AggregationClient(Configuration cfg) {
    this.conf = cfg;
  }

  /**
   * It gives the maximum value of a column for a given column family for the
   * given range. In case qualifier is null, a max of all values for the given
   * family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return max val <R>
   * @throws Throwable
   *           The caller is supposed to handle the exception as they are thrown
   *           & propagated to it.
   */
  public <R, S> R max(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    HTable table = new HTable(conf, tableName);

    class MaxCallBack implements Batch.Callback<R> {
      R max = null;

      R getMax() {
        return max;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        max = ci.compare(max, result) < 0 ? result : max;
      }
    }
    MaxCallBack aMaxCallBack = new MaxCallBack();
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<AggregateProtocol, R>() {
      @Override
      public R call(AggregateProtocol instance) throws IOException {
        return instance.getMax(ci, scan);
      }
    }, aMaxCallBack);
    return aMaxCallBack.getMax();
  }

  private void validateParameters(Scan scan) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
            .equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) {
      throw new IOException(
          "Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (scan.getFamilyMap().size() != 1) {
      throw new IOException("There must be only one family.");
    }
  }

  /**
   * It gives the minimum value of a column for a given column family for the
   * given range. In case qualifier is null, a min of all values for the given
   * family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return min val <R>
   * @throws Throwable
   */
  public <R, S> R min(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    class MinCallBack implements Batch.Callback<R> {

      private R min = null;

      public R getMinimum() {
        return min;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        min = (min == null || ci.compare(result, min) < 0) ? result : min;
      }
    }
    HTable table = new HTable(conf, tableName);
    MinCallBack minCallBack = new MinCallBack();
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<AggregateProtocol, R>() {

      @Override
      public R call(AggregateProtocol instance) throws IOException {
        return instance.getMin(ci, scan);
      }
    }, minCallBack);
    log.debug("Min fom all regions is: " + minCallBack.getMinimum());
    return minCallBack.getMinimum();
  }

  /**
   * It gives the row count, by summing up the individual results obtained from
   * regions. In case the qualifier is null, FirstKEyValueFilter is used to
   * optimised the operation. In case qualifier is provided, I can't use the
   * filter as it may set the flag to skip to next row, but the value read is
   * not of the given filter: in this case, this particular row will not be
   * counted ==> an error.
   * @param tableName
   * @param ci
   * @param scan
   * @return
   * @throws Throwable
   */
  public <R, S> long rowCount(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class RowNumCallback implements Batch.Callback<Long> {
      private final AtomicLong rowCountL = new AtomicLong(0);

      public long getRowNumCount() {
        return rowCountL.get();
      }

      @Override
      public void update(byte[] region, byte[] row, Long result) {
        rowCountL.addAndGet(result.longValue());
      }
    }
    RowNumCallback rowNum = new RowNumCallback();
    HTable table = new HTable(conf, tableName);
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<AggregateProtocol, Long>() {
      @Override
      public Long call(AggregateProtocol instance) throws IOException {
        return instance.getRowNum(ci, scan);
      }
    }, rowNum);
    return rowNum.getRowNumCount();
  }

  /**
   * It sums up the value returned from various regions. In case qualifier is
   * null, summation of all the column qualifiers in the given family is done.
   * @param tableName
   * @param ci
   * @param scan
   * @return sum <S>
   * @throws Throwable
   */
  public <R, S> S sum(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    class SumCallBack implements Batch.Callback<S> {
      S sumVal = null;

      public S getSumResult() {
        return sumVal;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, S result) {
        sumVal = ci.add(sumVal, result);
      }
    }
    SumCallBack sumCallBack = new SumCallBack();
    HTable table = new HTable(conf, tableName);
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<AggregateProtocol, S>() {
      @Override
      public S call(AggregateProtocol instance) throws IOException {
        return instance.getSum(ci, scan);
      }
    }, sumCallBack);
    return sumCallBack.getSumResult();
  }

  /**
   * It computes average while fetching sum and row count from all the
   * corresponding regions. Approach is to compute a global sum of region level
   * sum and rowcount and then compute the average.
   * @param tableName
   * @param scan
   * @throws Throwable
   */
  private <R, S> Pair<S, Long> getAvgArgs(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class AvgCallBack implements Batch.Callback<Pair<S, Long>> {
      S sum = null;
      Long rowCount = 0l;

      public Pair<S, Long> getAvgArgs() {
        return new Pair<S, Long>(sum, rowCount);
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<S, Long> result) {
        sum = ci.add(sum, result.getFirst());
        rowCount += result.getSecond();
      }
    }
    AvgCallBack avgCallBack = new AvgCallBack();
    HTable table = new HTable(conf, tableName);
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(), new Batch.Call<AggregateProtocol, Pair<S, Long>>() {
      @Override
      public Pair<S, Long> call(AggregateProtocol instance) throws IOException {
        return instance.getAvg(ci, scan);
      }
    }, avgCallBack);
    return avgCallBack.getAvgArgs();
  }

  /**
   * This is the client side interface/handle for calling the average method for
   * a given cf-cq combination. It was necessary to add one more call stack as
   * its return type should be a decimal value, irrespective of what
   * columninterpreter says. So, this methods collects the necessary parameters
   * to compute the average and returs the double value.
   * @param tableName
   * @param ci
   * @param scan
   * @return
   * @throws Throwable
   */
  public <R, S> double avg(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, Scan scan) throws Throwable {
    Pair<S, Long> p = getAvgArgs(tableName, ci, scan);
    return ci.divideForAvg(p.getFirst(), p.getSecond());
  }

  /**
   * It computes a global standard deviation for a given column and its value.
   * Standard deviation is square root of (average of squares -
   * average*average). From individual regions, it obtains sum, square sum and
   * number of rows. With these, the above values are computed to get the global
   * std.
   * @param tableName
   * @param scan
   * @return
   * @throws Throwable
   */
  private <R, S> Pair<List<S>, Long> getStdArgs(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class StdCallback implements Batch.Callback<Pair<List<S>, Long>> {
      long rowCountVal = 0l;
      S sumVal = null, sumSqVal = null;

      public Pair<List<S>, Long> getStdParams() {
        List<S> l = new ArrayList<S>();
        l.add(sumVal);
        l.add(sumSqVal);
        Pair<List<S>, Long> p = new Pair<List<S>, Long>(l, rowCountVal);
        return p;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<List<S>, Long> result) {
        sumVal = ci.add(sumVal, result.getFirst().get(0));
        sumSqVal = ci.add(sumSqVal, result.getFirst().get(1));
        rowCountVal += result.getSecond();
      }
    }
    StdCallback stdCallback = new StdCallback();
    HTable table = new HTable(conf, tableName);
    table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(), scan
        .getStopRow(),
        new Batch.Call<AggregateProtocol, Pair<List<S>, Long>>() {
          @Override
          public Pair<List<S>, Long> call(AggregateProtocol instance)
              throws IOException {
            return instance.getStd(ci, scan);
          }

        }, stdCallback);
    return stdCallback.getStdParams();
  }

  /**
   * This is the client side interface/handle for calling the std method for a
   * given cf-cq combination. It was necessary to add one more call stack as its
   * return type should be a decimal value, irrespective of what
   * columninterpreter says. So, this methods collects the necessary parameters
   * to compute the std and returns the double value.
   * @param tableName
   * @param ci
   * @param scan
   * @return
   * @throws Throwable
   */
  public <R, S> double std(final byte[] tableName, ColumnInterpreter<R, S> ci,
      Scan scan) throws Throwable {
    Pair<List<S>, Long> p = getStdArgs(tableName, ci, scan);
    double res = 0d;
    double avg = ci.divideForAvg(p.getFirst().get(0), p.getSecond());
    double avgOfSumSq = ci.divideForAvg(p.getFirst().get(1), p.getSecond());
    res = avgOfSumSq - (avg) * (avg); // variance
    res = Math.pow(res, 0.5);
    return res;
  }

}
