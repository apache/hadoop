package org.apache.hadoop.ozone.om.ratis;

import java.io.IOException;

/**
 * Functional interface for OM RatisSnapshot.
 */
public interface OzoneManagerRatisSnapshot {

  long takeSnapshot(long index) throws IOException;
}