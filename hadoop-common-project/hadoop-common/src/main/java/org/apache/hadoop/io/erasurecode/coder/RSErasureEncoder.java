package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawEncoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

/**
 * Reed-Solomon erasure encoder that encodes a block group.
 *
 * It implements {@link ErasureEncoder}.
 */
public class RSErasureEncoder extends AbstractErasureEncoder {
  private RawErasureEncoder rawEncoder;

  @Override
  protected ErasureCodingStep prepareEncodingStep(final ECBlockGroup blockGroup) {

    RawErasureEncoder rawEncoder = checkCreateRSRawEncoder();

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);

    return new ErasureEncodingStep(inputBlocks,
        getOutputBlocks(blockGroup), rawEncoder);
  }

  private RawErasureEncoder checkCreateRSRawEncoder() {
    if (rawEncoder == null) {
      rawEncoder = createRawEncoder(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_RAWCODER_KEY);
      if (rawEncoder == null) {
        rawEncoder = new RSRawEncoder();
      }
      rawEncoder.initialize(getNumDataUnits(),
          getNumParityUnits(), getChunkSize());
    }
    return rawEncoder;
  }

  @Override
  public void release() {
    if (rawEncoder != null) {
      rawEncoder.release();
    }
  }
}
