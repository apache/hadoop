package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;

/**
 * Signals that a pipeline is missing from PipelineManager.
 */
public class PipelineNotFoundException extends IOException{
  /**
   * Constructs an {@code PipelineNotFoundException} with {@code null}
   * as its error detail message.
   */
  public PipelineNotFoundException() {
    super();
  }

  /**
   * Constructs an {@code PipelineNotFoundException} with the specified
   * detail message.
   *
   * @param message
   *        The detail message (which is saved for later retrieval
   *        by the {@link #getMessage()} method)
   */
  public PipelineNotFoundException(String message) {
    super(message);
  }
}
