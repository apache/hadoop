package org.apache.hadoop.fs.impl;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Holds reference to an object to be attached to a stream or store to avoid
 * the reference being lost to GC.
 */
public class BackReference {
  private final Object reference;

  public BackReference(@Nullable Object reference) {
    this.reference = reference;
  }

  /**
   * is the reference null?
   * @return true if the ref. is null, else false.
   */
  public boolean isNull() {
    return reference == null;
  }

  @VisibleForTesting
  public Object getReference() {
    return reference;
  }

  @Override
  public String toString() {
    return "BackReference{" +
        "reference=" + reference +
        '}';
  }
}
