package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A filter for adding inter-column timestamp matching
 * Only cells with a correspondingly timestamped entry in
 * the target column will be retained
 * Not compatible with Scan.setBatch as operations need 
 * full rows for correct filtering 
 */
public class DependentColumnFilter extends CompareFilter {

  protected byte[] columnFamily;
  protected byte[] columnQualifier;
  protected boolean dropDependentColumn;

  protected Set<Long> stampSet = new HashSet<Long>();
  
  /**
   * Should only be used for writable
   */
  public DependentColumnFilter() {
  }
  
  /**
   * Build a dependent column filter with value checking
   * dependent column varies will be compared using the supplied
   * compareOp and comparator, for usage of which
   * refer to {@link CompareFilter}
   * 
   * @param family dependent column family
   * @param qualifier dependent column qualifier
   * @param dropDependentColumn whether the column should be discarded after
   * @param valueCompareOp comparison op 
   * @param valueComparator comparator
   */
  public DependentColumnFilter(final byte [] family, final byte[] qualifier,
		  final boolean dropDependentColumn, final CompareOp valueCompareOp,
	      final WritableByteArrayComparable valueComparator) {
    // set up the comparator   
    super(valueCompareOp, valueComparator);
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.dropDependentColumn = dropDependentColumn;
  }
  
  /**
   * Constructor for DependentColumn filter.
   * Keyvalues where a keyvalue from target column 
   * with the same timestamp do not exist will be dropped. 
   * 
   * @param family name of target column family
   * @param qualifier name of column qualifier
   */
  public DependentColumnFilter(final byte [] family, final byte [] qualifier) {
    this(family, qualifier, false);
  }
  
  /**
   * Constructor for DependentColumn filter.
   * Keyvalues where a keyvalue from target column 
   * with the same timestamp do not exist will be dropped. 
   * 
   * @param family name of dependent column family
   * @param qualifier name of dependent qualifier
   * @param dropDependentColumn whether the dependent columns keyvalues should be discarded
   */
  public DependentColumnFilter(final byte [] family, final byte [] qualifier,
      final boolean dropDependentColumn) {
    this(family, qualifier, dropDependentColumn, CompareOp.NO_OP, null);
  }
  
  
  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    // Check if the column and qualifier match
  	if (!v.matchingColumn(this.columnFamily, this.columnQualifier)) {
        // include non-matches for the time being, they'll be discarded afterwards
        return ReturnCode.INCLUDE;
  	}
  	// If it doesn't pass the op, skip it
  	if(comparator != null && doCompare(compareOp, comparator, v.getValue(), 0, v.getValueLength()))
  	  return ReturnCode.SKIP;  	  
	
    stampSet.add(v.getTimestamp());
    if(dropDependentColumn) {
    	return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    Iterator<KeyValue> it = kvs.iterator();
    KeyValue kv;
    while(it.hasNext()) {
      kv = it.next();
      if(!stampSet.contains(kv.getTimestamp())) {
        it.remove();
      }
    }
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }
  
  @Override
  public boolean filterRow() {
    return false;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    return false;
  }

  @Override
  public void reset() {
    stampSet.clear();    
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	super.readFields(in);
    this.columnFamily = Bytes.readByteArray(in);
	if(this.columnFamily.length == 0) {
	  this.columnFamily = null;
	}
    
    this.columnQualifier = Bytes.readByteArray(in);
    if(this.columnQualifier.length == 0) {
      this.columnQualifier = null;
    }	
    
    this.dropDependentColumn = in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Bytes.writeByteArray(out, this.columnFamily);
    Bytes.writeByteArray(out, this.columnQualifier);
    out.writeBoolean(this.dropDependentColumn);    
  }

}
