package org.apache.hadoop.hbase.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import junit.framework.TestCase;

/**
 * Testing the sizing that HeapSize offers and compares to the size given by
 * ClassSize. 
 */
public class TestHeapSize extends TestCase {
  static final Log LOG = LogFactory.getLog(TestHeapSize.class);
  // List of classes implementing HeapSize
  // BatchOperation, BatchUpdate, BlockIndex, Entry, Entry<K,V>, HStoreKey
  // KeyValue, LruBlockCache, LruHashMap<K,V>, Put, HLogKey
  
  /**
   * Testing the classes that implements HeapSize and are a part of 0.20. 
   * Some are not tested here for example BlockIndex which is tested in 
   * TestHFile since it is a non public class
   * @throws IOException 
   */
  @SuppressWarnings("unchecked")
  public void testSizes() throws IOException {
    Class cl = null;
    long expected = 0L;
    long actual = 0L;
    
    //KeyValue
    cl = KeyValue.class;
    expected = ClassSize.estimateBase(cl, false);
    
    KeyValue kv = new KeyValue();
    actual = kv.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    //LruBlockCache
    cl = LruBlockCache.class;
    expected = ClassSize.estimateBase(cl, false);
    LruBlockCache c = new LruBlockCache(1,1,200);
    //Since minimum size for the for a LruBlockCache is 1
    //we need to remove one reference from the heapsize
    actual = c.heapSize();// - ClassSize.REFERENCE_SIZE;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    //Put
    cl = Put.class;
    expected = ClassSize.estimateBase(cl, false);
    //The actual TreeMap is not included in the above calculation
    expected += ClassSize.TREEMAP;
    Put put = new Put(Bytes.toBytes(""));
    actual = put.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
  }

}
