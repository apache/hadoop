package org.apache.hadoop.io.serial.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * A raw comparator that compares byte strings in lexicographical order.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public final class MemcmpRawComparator implements RawComparator {
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
  }

}
