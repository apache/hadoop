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

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.DataVerifier.VerifyOutput;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;

/**
 * Operation which selects a random file and selects a random read size (from
 * the read size option) and reads from the start of that file to the read size
 * (or the full file) and verifies the bytes that were written there.
 * 
 * This operation will capture statistics on success the time taken to read that
 * file and the number of successful readings that occurred as well as the
 * number of bytes read and the number of chunks verified and the number of
 * chunks which failed verification and on failure or error it will capture the
 * number of failures and the amount of time taken to fail
 */
class ReadOp extends Operation {
  private static final Log LOG = LogFactory.getLog(ReadOp.class);

  ReadOp(ConfigExtractor cfg, Random rnd) {
    super(ReadOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Gets the file name to read
   * 
   * @return Path
   */
  protected Path getReadFile() {
    Path fn = getFinder().getFile();
    return fn;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    DataInputStream is = null;
    try {
      Path fn = getReadFile();
      Range<Long> readSizeRange = getConfig().getReadSize();
      long readSize = 0;
      String readStrAm = "";
      if (getConfig().shouldReadFullFile()) {
        readSize = Long.MAX_VALUE;
        readStrAm = "full file";
      } else {
        readSize = Range.betweenPositive(getRandom(), readSizeRange);
        readStrAm = Helper.toByteInfo(readSize);
      }
      long timeTaken = 0;
      long chunkSame = 0;
      long chunkDiff = 0;
      long bytesRead = 0;
      long startTime = 0;
      DataVerifier vf = new DataVerifier();
      LOG.info("Attempting to read file at " + fn + " of size (" + readStrAm
          + ")");
      {
        // open
        startTime = Timer.now();
        is = fs.open(fn);
        timeTaken += Timer.elapsed(startTime);
        // read & verify
        VerifyOutput vo = vf.verifyFile(readSize, is);
        timeTaken += vo.getReadTime();
        chunkSame += vo.getChunksSame();
        chunkDiff += vo.getChunksDifferent();
        bytesRead += vo.getBytesRead();
        // capture close time
        startTime = Timer.now();
        is.close();
        is = null;
        timeTaken += Timer.elapsed(startTime);
      }
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.OK_TIME_TAKEN, timeTaken));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.BYTES_READ, bytesRead));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.SUCCESSES, 1L));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.CHUNKS_VERIFIED, chunkSame));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.CHUNKS_UNVERIFIED, chunkDiff));
      LOG.info("Read " + Helper.toByteInfo(bytesRead) + " of " + fn + " with "
          + chunkSame + " chunks being same as expected and " + chunkDiff
          + " chunks being different than expected in " + timeTaken
          + " milliseconds");

    } catch (FileNotFoundException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.NOT_FOUND, 1L));
      LOG.warn("Error with reading", e);
    } catch (BadFileException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.BAD_FILES, 1L));
      LOG.warn("Error reading bad file", e);
    } catch (IOException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error reading", e);
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOG.warn("Error closing read stream", e);
        }
      }
    }
    return out;
  }
}
