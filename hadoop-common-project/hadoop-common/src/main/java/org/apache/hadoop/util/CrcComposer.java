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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Encapsulates logic for composing multiple CRCs into one or more combined CRCs
 * corresponding to concatenated underlying data ranges. Optimized for composing
 * a large number of CRCs that correspond to underlying chunks of data all of
 * same size.
 */
@InterfaceAudience.LimitedPrivate({"Common", "HDFS", "MapReduce", "Yarn"})
@InterfaceStability.Unstable
public class CrcComposer {
  private static final int CRC_SIZE_BYTES = 4;
  private static final Logger LOG = LoggerFactory.getLogger(CrcComposer.class);

  private final int crcPolynomial;
  private final int precomputedMonomialForHint;
  private final long bytesPerCrcHint;
  private final long stripeLength;

  private int curCompositeCrc = 0;
  private long curPositionInStripe = 0;
  private ByteArrayOutputStream digestOut = new ByteArrayOutputStream();

  /**
   * Returns a CrcComposer which will collapse all ingested CRCs into a single
   * value.
   */
  public static CrcComposer newCrcComposer(
      DataChecksum.Type type, long bytesPerCrcHint)
      throws IOException {
    return newStripedCrcComposer(type, bytesPerCrcHint, Long.MAX_VALUE);
  }

  /**
   * Returns a CrcComposer which will collapse CRCs for every combined
   * underlying data size which aligns with the specified stripe boundary. For
   * example, if "update" is called with 20 CRCs and bytesPerCrc == 5, and
   * stripeLength == 10, then every two (10 / 5) consecutive CRCs will be
   * combined with each other, yielding a list of 10 CRC "stripes" in the
   * final digest, each corresponding to 10 underlying data bytes. Using
   * a stripeLength greater than the total underlying data size is equivalent
   * to using a non-striped CrcComposer.
   */
  public static CrcComposer newStripedCrcComposer(
      DataChecksum.Type type, long bytesPerCrcHint, long stripeLength)
      throws IOException {
    int polynomial = DataChecksum.getCrcPolynomialForType(type);
    return new CrcComposer(
        polynomial,
        CrcUtil.getMonomial(bytesPerCrcHint, polynomial),
        bytesPerCrcHint,
        stripeLength);
  }

  CrcComposer(
      int crcPolynomial,
      int precomputedMonomialForHint,
      long bytesPerCrcHint,
      long stripeLength) {
    LOG.debug(
        "crcPolynomial=0x{}, precomputedMonomialForHint=0x{}, "
        + "bytesPerCrcHint={}, stripeLength={}",
        Integer.toString(crcPolynomial, 16),
        Integer.toString(precomputedMonomialForHint, 16),
        bytesPerCrcHint,
        stripeLength);
    this.crcPolynomial = crcPolynomial;
    this.precomputedMonomialForHint = precomputedMonomialForHint;
    this.bytesPerCrcHint = bytesPerCrcHint;
    this.stripeLength = stripeLength;
  }

  /**
   * Composes length / CRC_SIZE_IN_BYTES more CRCs from crcBuffer, with
   * each CRC expected to correspond to exactly {@code bytesPerCrc} underlying
   * data bytes.
   *
   * @param length must be a multiple of the expected byte-size of a CRC.
   */
  public void update(
      byte[] crcBuffer, int offset, int length, long bytesPerCrc)
      throws IOException {
    if (length % CRC_SIZE_BYTES != 0) {
      throw new IOException(String.format(
          "Trying to update CRC from byte array with length '%d' at offset "
          + "'%d' which is not a multiple of %d!",
          length, offset, CRC_SIZE_BYTES));
    }
    int limit = offset + length;
    while (offset < limit) {
      int crcB = CrcUtil.readInt(crcBuffer, offset);
      update(crcB, bytesPerCrc);
      offset += CRC_SIZE_BYTES;
    }
  }

  /**
   * Composes {@code numChecksumsToRead} additional CRCs into the current digest
   * out of {@code checksumIn}, with each CRC expected to correspond to exactly
   * {@code bytesPerCrc} underlying data bytes.
   */
  public void update(
      DataInputStream checksumIn, long numChecksumsToRead, long bytesPerCrc)
      throws IOException {
    for (long i = 0; i < numChecksumsToRead; ++i) {
      int crcB = checksumIn.readInt();
      update(crcB, bytesPerCrc);
    }
  }

  /**
   * Updates with a single additional CRC which corresponds to an underlying
   * data size of {@code bytesPerCrc}.
   */
  public void update(int crcB, long bytesPerCrc) throws IOException {
    if (curCompositeCrc == 0) {
      curCompositeCrc = crcB;
    } else if (bytesPerCrc == bytesPerCrcHint) {
      curCompositeCrc = CrcUtil.composeWithMonomial(
          curCompositeCrc, crcB, precomputedMonomialForHint, crcPolynomial);
    } else {
      curCompositeCrc = CrcUtil.compose(
          curCompositeCrc, crcB, bytesPerCrc, crcPolynomial);
    }

    curPositionInStripe += bytesPerCrc;

    if (curPositionInStripe > stripeLength) {
      throw new IOException(String.format(
          "Current position in stripe '%d' after advancing by bytesPerCrc '%d' "
          + "exceeds stripeLength '%d' without stripe alignment.",
          curPositionInStripe, bytesPerCrc, stripeLength));
    } else if (curPositionInStripe == stripeLength) {
      // Hit a stripe boundary; flush the curCompositeCrc and reset for next
      // stripe.
      digestOut.write(CrcUtil.intToBytes(curCompositeCrc), 0, CRC_SIZE_BYTES);
      curCompositeCrc = 0;
      curPositionInStripe = 0;
    }
  }

  /**
   * Returns byte representation of composed CRCs; if no stripeLength was
   * specified, the digest should be of length equal to exactly one CRC.
   * Otherwise, the number of CRCs in the returned array is equal to the
   * total sum bytesPerCrc divided by stripeLength. If the sum of bytesPerCrc
   * is not a multiple of stripeLength, then the last CRC in the array
   * corresponds to totalLength % stripeLength underlying data bytes.
   */
  public byte[] digest() {
    if (curPositionInStripe > 0) {
      digestOut.write(CrcUtil.intToBytes(curCompositeCrc), 0, CRC_SIZE_BYTES);
      curCompositeCrc = 0;
      curPositionInStripe = 0;
    }
    byte[] digestValue = digestOut.toByteArray();
    digestOut.reset();
    return digestValue;
  }
}
