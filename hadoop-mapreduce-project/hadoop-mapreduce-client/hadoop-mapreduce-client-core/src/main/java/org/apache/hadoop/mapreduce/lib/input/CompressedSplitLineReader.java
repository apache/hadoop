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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;

/**
 * Line reader for compressed splits
 *
 * Reading records from a compressed split is tricky, as the
 * LineRecordReader is using the reported compressed input stream
 * position directly to determine when a split has ended.  In addition the
 * compressed input stream is usually faking the actual byte position, often
 * updating it only after the first compressed block after the split is
 * accessed.
 *
 * Depending upon where the last compressed block of the split ends relative
 * to the record delimiters it can be easy to accidentally drop the last
 * record or duplicate the last record between this split and the next.
 *
 * Split end scenarios:
 *
 * 1) Last block of split ends in the middle of a record
 *      Nothing special that needs to be done here, since the compressed input
 *      stream will report a position after the split end once the record
 *      is fully read.  The consumer of the next split will discard the
 *      partial record at the start of the split normally, and no data is lost
 *      or duplicated between the splits.
 *
 * 2) Last block of split ends in the middle of a delimiter
 *      The line reader will continue to consume bytes into the next block to
 *      locate the end of the delimiter.  If a custom delimiter is being used
 *      then the next record must be read by this split or it will be dropped.
 *      The consumer of the next split will not recognize the partial
 *      delimiter at the beginning of its split and will discard it along with
 *      the next record.
 *
 *      However for the default delimiter processing there is a special case
 *      because CR, LF, and CRLF are all valid record delimiters.  If the
 *      block ends with a CR then the reader must peek at the next byte to see
 *      if it is an LF and therefore part of the same record delimiter.
 *      Peeking at the next byte is an access to the next block and triggers
 *      the stream to report the end of the split.  There are two cases based
 *      on the next byte:
 *
 *      A) The next byte is LF
 *           The split needs to end after the current record is returned.  The
 *           consumer of the next split will discard the first record, which
 *           is degenerate since LF is itself a delimiter, and start consuming
 *           records after that byte.  If the current split tries to read
 *           another record then the record will be duplicated between splits.
 *
 *      B) The next byte is not LF
 *           The current record will be returned but the stream will report
 *           the split has ended due to the peek into the next block.  If the
 *           next record is not read then it will be lost, as the consumer of
 *           the next split will discard it before processing subsequent
 *           records.  Therefore the next record beyond the reported split end
 *           must be consumed by this split to avoid data loss.
 *
 * 3) Last block of split ends at the beginning of a delimiter
 *      This is equivalent to case 1, as the reader will consume bytes into
 *      the next block and trigger the end of the split.  No further records
 *      should be read as the consumer of the next split will discard the
 *      (degenerate) record at the beginning of its split.
 *
 * 4) Last block of split ends at the end of a delimiter
 *      Nothing special needs to be done here. The reader will not start
 *      examining the bytes into the next block until the next record is read,
 *      so the stream will not report the end of the split just yet.  Once the
 *      next record is read then the next block will be accessed and the
 *      stream will indicate the end of the split.  The consumer of the next
 *      split will correctly discard the first record of its split, and no
 *      data is lost or duplicated.
 *
 *      If the default delimiter is used and the block ends at a CR then this
 *      is treated as case 2 since the reader does not yet know without
 *      looking at subsequent bytes whether the delimiter has ended.
 *
 * NOTE: It is assumed that compressed input streams *never* return bytes from
 *       multiple compressed blocks from a single read.  Failure to do so will
 *       violate the buffering performed by this class, as it will access
 *       bytes into the next block after the split before returning all of the
 *       records from the previous block.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CompressedSplitLineReader extends SplitLineReader {

  SplitCompressionInputStream scin;
  private boolean usingCRLF;
  private boolean needAdditionalRecord = false;
  private boolean finished = false;

  public CompressedSplitLineReader(SplitCompressionInputStream in,
                                   Configuration conf,
                                   byte[] recordDelimiterBytes)
                                       throws IOException {
    super(in, conf, recordDelimiterBytes);
    scin = in;
    usingCRLF = (recordDelimiterBytes == null);
  }

  @Override
  protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter)
      throws IOException {
    int bytesRead = in.read(buffer);

    // If the split ended in the middle of a record delimiter then we need
    // to read one additional record, as the consumer of the next split will
    // not recognize the partial delimiter as a record.
    // However if using the default delimiter and the next character is a
    // linefeed then next split will treat it as a delimiter all by itself
    // and the additional record read should not be performed.
    if (inDelimiter && bytesRead > 0) {
      if (usingCRLF) {
        needAdditionalRecord = (buffer[0] != '\n');
      } else {
        needAdditionalRecord = true;
      }
    }
    return bytesRead;
  }

  @Override
  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {
    int bytesRead = 0;
    if (!finished) {
      // only allow at most one more record to be read after the stream
      // reports the split ended
      if (scin.getPos() > scin.getAdjustedEnd()) {
        finished = true;
      }

      bytesRead = super.readLine(str, maxLineLength, maxBytesToConsume);
    }
    return bytesRead;
  }

  @Override
  public boolean needAdditionalRecordAfterSplit() {
    return !finished && needAdditionalRecord;
  }

  @Override
  protected void unsetNeedAdditionalRecordAfterSplit() {
    needAdditionalRecord = false;
  }
}
