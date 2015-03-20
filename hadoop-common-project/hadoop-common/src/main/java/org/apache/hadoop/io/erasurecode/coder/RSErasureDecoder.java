package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.rawcoder.JRSRawDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.XorRawDecoder;

/**
 * Reed-Solomon erasure decoder that decodes a block group.
 *
 * It implements {@link ErasureDecoder}.
 */
public class RSErasureDecoder extends AbstractErasureDecoder {
  private RawErasureDecoder rsRawDecoder;
  private RawErasureDecoder xorRawDecoder;
  private boolean useXorWhenPossible = true;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);

    if (conf != null) {
      this.useXorWhenPossible = conf.getBoolean(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_USEXOR_KEY, true);
    }
  }

    @Override
  protected ErasureCodingStep prepareDecodingStep(final ECBlockGroup blockGroup) {

    RawErasureDecoder rawDecoder;

    ECBlock[] inputBlocks = getInputBlocks(blockGroup);
    ECBlock[] outputBlocks = getOutputBlocks(blockGroup);

    /**
     * Optimization: according to some benchmark, when only one block is erased
     * and to be recovering, the most simple XOR scheme can be much efficient.
     * We will have benchmark tests to verify this opt is effect or not.
     */
    if (outputBlocks.length == 1 && useXorWhenPossible) {
      rawDecoder = checkCreateXorRawDecoder();
    } else {
      rawDecoder = checkCreateRSRawDecoder();
    }

    return new ErasureDecodingStep(inputBlocks,
        getErasedIndexes(inputBlocks), outputBlocks, rawDecoder);
  }

  private RawErasureDecoder checkCreateRSRawDecoder() {
    if (rsRawDecoder == null) {
      rsRawDecoder = createRawDecoder(
          CommonConfigurationKeys.IO_ERASURECODE_CODEC_RS_RAWCODER_KEY);
      if (rsRawDecoder == null) {
        rsRawDecoder = new JRSRawDecoder();
      }
      rsRawDecoder.initialize(getNumDataUnits(),
          getNumParityUnits(), getChunkSize());
    }
    return rsRawDecoder;
  }

  private RawErasureDecoder checkCreateXorRawDecoder() {
    if (xorRawDecoder == null) {
      xorRawDecoder = new XorRawDecoder();
      xorRawDecoder.initialize(getNumDataUnits(), 1, getChunkSize());
    }
    return xorRawDecoder;
  }

  @Override
  public void release() {
    if (xorRawDecoder != null) {
      xorRawDecoder.release();
    } else if (rsRawDecoder != null) {
      rsRawDecoder.release();
    }
  }
}
