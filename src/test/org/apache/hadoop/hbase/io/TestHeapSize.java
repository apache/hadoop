package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
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
   * Test our hard-coded sizing of native java objects
   */
  @SuppressWarnings("unchecked")
  public void testNativeSizes() throws IOException {
    Class cl = null;
    long expected = 0L;
    long actual = 0L;
    
    // ArrayList
    cl = ArrayList.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ARRAYLIST;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // ByteBuffer
    cl = ByteBuffer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.BYTE_BUFFER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // Integer
    cl = Integer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.INTEGER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // Map.Entry
    // Interface is public, all others are not.  Hard to size via ClassSize
//    cl = Map.Entry.class;
//    expected = ClassSize.estimateBase(cl, false);
//    actual = ClassSize.MAP_ENTRY;
//    if(expected != actual) {
//      ClassSize.estimateBase(cl, true);
//      assertEquals(expected, actual);
//    }
    
    // Object
    cl = Object.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.OBJECT;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // TreeMap
    cl = TreeMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.TREEMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // String
    cl = String.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.STRING;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
  }
  
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
    
    //LruBlockCache Overhead
    cl = LruBlockCache.class;
    actual = LruBlockCache.CACHE_FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // LruBlockCache Map Fixed Overhead
    cl = ConcurrentHashMap.class;
    actual = ClassSize.CONCURRENT_HASHMAP;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
    
    // CachedBlock Fixed Overhead
    // We really need "deep" sizing but ClassSize does not do this.
    // Perhaps we should do all these more in this style....
    cl = CachedBlock.class;
    actual = CachedBlock.PER_BLOCK_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(String.class, false);
    expected += ClassSize.estimateBase(ByteBuffer.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(String.class, true);
      ClassSize.estimateBase(ByteBuffer.class, true);
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
