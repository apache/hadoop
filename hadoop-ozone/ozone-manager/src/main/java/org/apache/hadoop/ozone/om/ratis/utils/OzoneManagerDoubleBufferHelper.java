package org.apache.hadoop.ozone.om.ratis.utils;

import org.apache.hadoop.ozone.om.response.OMClientResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Helper interface for OzoneManagerDoubleBuffer.
 *
 */
public interface OzoneManagerDoubleBufferHelper {

  CompletableFuture<Void> add(OMClientResponse response,
      long transactionIndex);
}
