package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Class that provides static method needed when putting deletes into memcache 
 */
public class DeleteCompare {
  
  /**
   * Return codes from deleteCompare.
   */
  enum DeleteCode {
    /**
     * Do nothing.  Move to next KV in Memcache
     */
    SKIP,
    
    /**
     * Add to the list of deletes.
     */
    DELETE,
    
    /**
     * Stop looking at KVs in Memcache.  Finalize.
     */
    DONE
  }

  /**
   * Method used when putting deletes into memcache to remove all the previous
   * entries that are affected by this Delete
   * @param mem
   * @param deleteBuffer
   * @param deleteRowOffset
   * @param deleteRowLength
   * @param deleteQualifierOffset
   * @param deleteQualifierLength
   * @param deleteTimeOffset
   * @param deleteType
   * @param comparator
   * @return SKIP if current KeyValue should not be deleted, DELETE if
   * current KeyValue should be deleted and DONE when the current KeyValue is
   * out of the Deletes range
   */
  public static DeleteCode deleteCompare(KeyValue mem, byte [] deleteBuffer,
      int deleteRowOffset, short deleteRowLength, int deleteQualifierOffset,
      int deleteQualifierLength, int deleteTimeOffset, byte deleteType,
      KeyValue.KeyComparator comparator) {

    //Parsing new KeyValue
    byte [] memBuffer = mem.getBuffer();
    int memOffset = mem.getOffset();

    //Getting key lengths
    int memKeyLen = Bytes.toInt(memBuffer, memOffset);
    memOffset += Bytes.SIZEOF_INT;

    //Skipping value lengths
    memOffset += Bytes.SIZEOF_INT;

    //Getting row lengths
    short memRowLen = Bytes.toShort(memBuffer, memOffset);
    memOffset += Bytes.SIZEOF_SHORT;
    int res = comparator.compareRows(memBuffer, memOffset, memRowLen,
        deleteBuffer, deleteRowOffset, deleteRowLength);
    if(res > 0) {
      return DeleteCode.DONE;
    } else if(res < 0){
      System.out.println("SKIPPING ROW");
      return DeleteCode.SKIP;
    }

    memOffset += memRowLen;

    //Getting family lengths
    byte memFamLen = memBuffer[memOffset];
    memOffset += Bytes.SIZEOF_BYTE + memFamLen;

    //Get column lengths
    int memQualifierLen = memKeyLen - memRowLen - memFamLen -
      Bytes.SIZEOF_SHORT - Bytes.SIZEOF_BYTE - Bytes.SIZEOF_LONG -
      Bytes.SIZEOF_BYTE;

    //Compare timestamp
    int tsOffset = memOffset + memQualifierLen;
    int timeRes = Bytes.compareTo(memBuffer, tsOffset, Bytes.SIZEOF_LONG,
        deleteBuffer, deleteTimeOffset, Bytes.SIZEOF_LONG);

    if (deleteType == KeyValue.Type.DeleteFamily.getCode()) {
      if (timeRes <= 0) {
        return DeleteCode.DELETE;
      }
      return DeleteCode.SKIP;
    }

    //Compare columns
    res = Bytes.compareTo(memBuffer, memOffset, memQualifierLen,
        deleteBuffer, deleteQualifierOffset, deleteQualifierLength);
    if (res < 0) {
      return DeleteCode.SKIP;
    } else if(res > 0) {
      return DeleteCode.DONE;
    }
    // same column, compare the time.
    if (timeRes == 0) {
      return DeleteCode.DELETE;
    } else if (timeRes < 0) {
      if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
        return DeleteCode.DELETE;
      }
      return DeleteCode.DONE;
    } else {
      System.out.println("SKIPPING TS");
      return DeleteCode.SKIP;
    }
  } 
}
