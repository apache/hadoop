/**
 * 
 */
package org.apache.hadoop.hbase.migration.nineteen.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.migration.nineteen.HStoreKey;
import org.apache.hadoop.io.Writable;

/**
 * A reference to a part of a store file.  The file referenced usually lives
 * under a different region.  The part referenced is usually the top or bottom
 * half of the file.  References are made at region split time.  Being lazy
 * about copying data between the parent of the split and the split daughters
 * makes splitting faster.
 * 
 * <p>References work with {@link HalfMapFileReader}.  References know how to
 * write out the reference format in the file system and are whats juggled when
 * references are mixed in with direct store files.  The
 * {@link HalfMapFileReader} is used reading the referred to file.
 *
 * <p>References to store files located over in some other region look like
 * this in the file system
 * <code>1278437856009925445.hbaserepository,qAReLZD-OyQORZWq_vqR1k==,959247014679548184</code>:
 * i.e. an id followed by the name of the referenced region.  The data
 * ('mapfiles') of references are empty. The accompanying <code>info</code> file
 * contains the <code>midkey</code> that demarks top and bottom of the
 * referenced storefile, the id of the remote store we're referencing and
 * whether we're to serve the top or bottom region of the remote store file.
 * Note, a region is itself not splitable if it has instances of store file
 * references.  References are cleaned up by compactions.
 */
public class Reference implements Writable {
  // TODO: see if it makes sense making a ReferenceMapFile whose Writer is this
  // class and whose Reader is the {@link HalfMapFileReader}.

  private int encodedRegionName;
  private long fileid;
  private Range region;
  private HStoreKey midkey;
  
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
  
 public Reference(final int ern, final long fid, final HStoreKey m,
      final Range fr) {
    this.encodedRegionName = ern;
    this.fileid = fid;
    this.region = fr;
    this.midkey = m;
  }
  
 public Reference() {
    this(-1, -1, null, Range.bottom);
  }

  public long getFileId() {
    return fileid;
  }

  public Range getFileRegion() {
    return region;
  }
  
  public HStoreKey getMidkey() {
    return midkey;
  }
  
  public int getEncodedRegionName() {
    return this.encodedRegionName;
  }

  @Override
  public String toString() {
    return encodedRegionName + "/" + fileid + "/" + region;
  }

  // Make it serializable.

  public void write(DataOutput out) throws IOException {
    // Write out the encoded region name as a String.  Doing it as a String
    // keeps a Reference's serialization backword compatible with
    // pre-HBASE-82 serializations.  ALternative is rewriting all
    // info files in hbase (Serialized References are written into the
    // 'info' file that accompanies HBase Store files).
    out.writeUTF(Integer.toString(encodedRegionName));
    out.writeLong(fileid);
    // Write true if we're doing top of the file.
    out.writeBoolean(isTopFileRegion(region));
    this.midkey.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.encodedRegionName = Integer.parseInt(in.readUTF());
    fileid = in.readLong();
    boolean tmp = in.readBoolean();
    // If true, set region to top.
    region = tmp? Range.top: Range.bottom;
    midkey = new HStoreKey();
    midkey.readFields(in);
  }
  
  public static boolean isTopFileRegion(final Range r) {
    return r.equals(Range.top);
  }
}