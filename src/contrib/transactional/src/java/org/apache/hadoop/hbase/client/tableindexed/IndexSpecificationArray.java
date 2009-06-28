package org.apache.hadoop.hbase.client.tableindexed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/** Holds an array of index specifications.
 * 
 */
public class IndexSpecificationArray implements Writable {

  private IndexSpecification [] indexSpecifications;
  
  public IndexSpecificationArray() {
    // FOr writable
  }
  public IndexSpecificationArray(IndexSpecification[] specs) {
    this.indexSpecifications = specs;
  }
 
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    indexSpecifications = new IndexSpecification[size];
    for (int i=0; i<size; i++) {
      indexSpecifications[i] = new IndexSpecification();
      indexSpecifications[i].readFields(in);
    }

  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(indexSpecifications.length);
    for (IndexSpecification indexSpec : indexSpecifications) {
      indexSpec.write(out);
    }
  }
  /** Get indexSpecifications.
   * @return indexSpecifications
   */
  public IndexSpecification[] getIndexSpecifications() {
    return indexSpecifications;
  }

}
