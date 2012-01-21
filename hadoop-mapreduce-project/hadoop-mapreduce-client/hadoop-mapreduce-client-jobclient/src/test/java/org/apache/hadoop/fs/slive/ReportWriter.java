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

import java.io.PrintWriter;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class which provides a report for the given operation output
 */
class ReportWriter {

  // simple measurement types
  // expect long values
  // these will be reported on + rates by this reporter
  static final String OK_TIME_TAKEN = "milliseconds_taken";
  static final String FAILURES = "failures";
  static final String SUCCESSES = "successes";
  static final String BYTES_WRITTEN = "bytes_written";
  static final String FILES_CREATED = "files_created";
  static final String DIR_ENTRIES = "dir_entries";
  static final String OP_COUNT = "op_count";
  static final String CHUNKS_VERIFIED = "chunks_verified";
  static final String CHUNKS_UNVERIFIED = "chunks_unverified";
  static final String BYTES_READ = "bytes_read";
  static final String NOT_FOUND = "files_not_found";
  static final String BAD_FILES = "bad_files";

  private static final Log LOG = LogFactory.getLog(ReportWriter.class);

  private static final String SECTION_DELIM = "-------------";

  /**
   * @return String to be used for as a section delimiter
   */
  private String getSectionDelimiter() {
    return SECTION_DELIM;
  }

  /**
   * Writes a message the the logging library and the given print writer (if it
   * is not null)
   * 
   * @param msg
   *          the message to write
   * @param os
   *          the print writer if specified to also write to
   */
  private void writeMessage(String msg, PrintWriter os) {
    LOG.info(msg);
    if (os != null) {
      os.println(msg);
    }
  }

  /**
   * Provides a simple report showing only the input size, and for each
   * operation the operation type, measurement type and its values.
   * 
   * @param input
   *          the list of operations to report on
   * @param os
   *          any print writer for which output should be written to (along with
   *          the logging library)
   */
  void basicReport(List<OperationOutput> input, PrintWriter os) {
    writeMessage("Default report for " + input.size() + " operations ", os);
    writeMessage(getSectionDelimiter(), os);
    for (OperationOutput data : input) {
      writeMessage("Operation \"" + data.getOperationType() + "\" measuring \""
          + data.getMeasurementType() + "\" = " + data.getValue(), os);
    }
    writeMessage(getSectionDelimiter(), os);
  }

  /**
   * Provides a more detailed report for a given operation. This will output the
   * keys and values for all input and then sort based on measurement type and
   * attempt to show rates for various metrics which have expected types to be
   * able to measure there rate. Currently this will show rates for bytes
   * written, success count, files created, directory entries, op count and
   * bytes read if the variable for time taken is available for each measurement
   * type.
   * 
   * @param operation
   *          the operation that is being reported on.
   * @param input
   *          the set of data for that that operation.
   * @param os
   *          any print writer for which output should be written to (along with
   *          the logging library)
   */
  void opReport(String operation, List<OperationOutput> input,
      PrintWriter os) {
    writeMessage("Basic report for operation type " + operation, os);
    writeMessage(getSectionDelimiter(), os);
    for (OperationOutput data : input) {
      writeMessage("Measurement \"" + data.getMeasurementType() + "\" = "
          + data.getValue(), os);
    }
    // split up into measurement types for rates...
    Map<String, OperationOutput> combined = new TreeMap<String, OperationOutput>();
    for (OperationOutput data : input) {
      if (combined.containsKey(data.getMeasurementType())) {
        OperationOutput curr = combined.get(data.getMeasurementType());
        combined.put(data.getMeasurementType(), OperationOutput.merge(curr,
            data));
      } else {
        combined.put(data.getMeasurementType(), data);
      }
    }
    // handle the known types
    OperationOutput timeTaken = combined.get(OK_TIME_TAKEN);
    if (timeTaken != null) {
      Long mTaken = Long.parseLong(timeTaken.getValue().toString());
      if (mTaken > 0) {
        NumberFormat formatter = Formatter.getDecimalFormatter();
        for (String measurementType : combined.keySet()) {
          Double rate = null;
          String rateType = "";
          if (measurementType.equals(BYTES_WRITTEN)) {
            Long mbWritten = Long.parseLong(combined.get(measurementType)
                .getValue().toString())
                / (Constants.MEGABYTES);
            rate = (double) mbWritten / (double) (mTaken / 1000.0d);
            rateType = "MB/sec";
          } else if (measurementType.equals(SUCCESSES)) {
            Long succ = Long.parseLong(combined.get(measurementType).getValue()
                .toString());
            rate = (double) succ / (double) (mTaken / 1000.0d);
            rateType = "successes/sec";
          } else if (measurementType.equals(FILES_CREATED)) {
            Long filesCreated = Long.parseLong(combined.get(measurementType)
                .getValue().toString());
            rate = (double) filesCreated / (double) (mTaken / 1000.0d);
            rateType = "files created/sec";
          } else if (measurementType.equals(DIR_ENTRIES)) {
            Long entries = Long.parseLong(combined.get(measurementType)
                .getValue().toString());
            rate = (double) entries / (double) (mTaken / 1000.0d);
            rateType = "directory entries/sec";
          } else if (measurementType.equals(OP_COUNT)) {
            Long opCount = Long.parseLong(combined.get(measurementType)
                .getValue().toString());
            rate = (double) opCount / (double) (mTaken / 1000.0d);
            rateType = "operations/sec";
          } else if (measurementType.equals(BYTES_READ)) {
            Long mbRead = Long.parseLong(combined.get(measurementType)
                .getValue().toString())
                / (Constants.MEGABYTES);
            rate = (double) mbRead / (double) (mTaken / 1000.0d);
            rateType = "MB/sec";
          }
          if (rate != null) {
            writeMessage("Rate for measurement \"" + measurementType + "\" = "
                + formatter.format(rate) + " " + rateType, os);
          }
        }
      }
    }
    writeMessage(getSectionDelimiter(), os);
  }

}
