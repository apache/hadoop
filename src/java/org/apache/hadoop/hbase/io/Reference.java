/**
 * 
 */
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;

/**
 * A reference to the top or bottom half of a store file.  The file referenced
 * lives under a different region.  References are made at region split time.
 * 
 * <p>References work with a special half store file type.  References know how
 * to write out the reference format in the file system and are whats juggled
 * when references are mixed in with direct store files.  The half store file
 * type is used reading the referred to file.
 *
 * <p>References to store files located over in some other region look like
 * this in the file system
 * <code>1278437856009925445.3323223323</code>:
 * i.e. an id followed by hash of the referenced region.
 * Note, a region is itself not splitable if it has instances of store file
 * references.  References are cleaned up by compactions.
 */
public class Reference implements Writable {
  private byte [] splitkey;
  private Range region;

  /** 
   * For split HStoreFiles, it specifies if the file covers the lower half or
   * the upper half of the key range
   */
  public static enum Range {
    /** HStoreFile contains upper half of key range */
    top,
    /** HStoreFile contains lower half of key range */
    bottom
  }

  /**
   * Constructor
   * @param r
   * @param s This is a serialized storekey with the row we are to split on,
   * an empty column and a timestamp of the LATEST_TIMESTAMP.  This is the first
   * possible entry in a row.  This is what we are splitting around.
   * @param fr
   */
  public Reference(final byte [] s, final Range fr) {
    this.splitkey = s;
    this.region = fr;
  }

  /**
   * Used by serializations.
   */
  public Reference() {
    this(null, Range.bottom);
  }

  public Range getFileRegion() {
    return this.region;
  }

  public byte [] getSplitKey() {
    return splitkey;
  }

  public String toString() {
    return "" + this.region;
  }

  // Make it serializable.

  public void write(DataOutput out) throws IOException {
    // Write true if we're doing top of the file.
    out.writeBoolean(isTopFileRegion(this.region));
    Bytes.writeByteArray(out, this.splitkey);
  }

  public void readFields(DataInput in) throws IOException {
    boolean tmp = in.readBoolean();
    // If true, set region to top.
    this.region = tmp? Range.top: Range.bottom;
    this.splitkey = Bytes.readByteArray(in);
  }

  public static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }

  public Path write(final FileSystem fs, final Path p)
  throws IOException {
    FSUtils.create(fs, p);
    FSDataOutputStream out = fs.create(p);
    try {
      write(out);
    } finally {
      out.close();
    }
    return p;
  }

  /**
   * Read a Reference from FileSystem.
   * @param fs
   * @param p
   * @return New Reference made from passed <code>p</code>
   * @throws IOException
   */
  public static Reference read(final FileSystem fs, final Path p)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      Reference r = new Reference();
      r.readFields(in);
      return r;
    } finally {
      in.close();
    }
  }
}