package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;

import junit.framework.TestCase;

/**
 * Test class fosr the Store 
 */
public class TestStore extends TestCase {
  Store store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf2 = Bytes.toBytes("qf2");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf4 = Bytes.toBytes("qf4");
  byte [] qf5 = Bytes.toBytes("qf5");
  byte [] qf6 = Bytes.toBytes("qf6");

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<KeyValue> expected = new ArrayList<KeyValue>();
  List<KeyValue> result = new ArrayList<KeyValue>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private final String DIR = "test/build/data/TestStore/";

  /**
   * Setup
   * @throws IOException
   */
  @Override
  public void setUp() throws IOException {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, null));
      get.addColumn(family, next);
    }
  }

  private void init(String methodName) throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    HBaseConfiguration conf = new HBaseConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path reconstructionLog = null; 
    Progressable reporter = null;

    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd, null, null, false);

    store = new Store(basedir, info, hcd, fs, reconstructionLog, conf,
        reporter);
  }

  
  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Getting data from memstore only
   * @throws IOException
   */
  public void testGet_FromMemStoreOnly() throws IOException {
    init(this.getName());
    
    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, null));
    this.store.add(new KeyValue(row, family, qf2, null));
    this.store.add(new KeyValue(row, family, qf3, null));
    this.store.add(new KeyValue(row, family, qf4, null));
    this.store.add(new KeyValue(row, family, qf5, null));
    this.store.add(new KeyValue(row, family, qf6, null));

    //Get
    this.store.get(get, qualifiers, result);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from files only
   * @throws IOException
   */
  public void testGet_FromFilesOnly() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, null));
    this.store.add(new KeyValue(row, family, qf2, null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, null));
    this.store.add(new KeyValue(row, family, qf4, null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, null));
    this.store.add(new KeyValue(row, family, qf6, null));
    //flush
    flush(3);

    //Get
    this.store.get(get, qualifiers, result);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  public void testGet_FromMemStoreAndFiles() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, null));
    this.store.add(new KeyValue(row, family, qf2, null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, null));
    this.store.add(new KeyValue(row, family, qf4, null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, null));
    this.store.add(new KeyValue(row, family, qf6, null));

    //Get
    this.store.get(get, qualifiers, result);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

    //Compare
    assertCheck();
  }

  private void flush(int storeFilessize) throws IOException{
    this.store.snapshot();
    this.store.flushCache(id++);
    assertEquals(storeFilessize, this.store.getStorefiles().size());
    assertEquals(0, this.store.memstore.kvset.size());
  }

  private void assertCheck() {
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // IncrementColumnValue tests
  //////////////////////////////////////////////////////////////////////////////
  /**
   * Testing if the update in place works. When you want to update a value that
   * is already in memstore, you don't delete it and put a new one, but just 
   * update the value in the original KeyValue
   * @throws IOException
   */
  public void testIncrementColumnValue_UpdatingInPlace() throws IOException {
    init(this.getName());

    //Put data in memstore
    long value = 1L;
    long amount = 3L;
    this.store.add(new KeyValue(row, family, qf1, Bytes.toBytes(value)));
    
    Store.ICVResult vas = this.store.incrementColumnValue(row, family, qf1, amount);
    assertEquals(vas.value, value+amount);
    store.add(vas.kv);
    Get get = new Get(row);
    get.addColumn(family, qf1);
    NavigableSet<byte[]> qualifiers = 
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qf1);
    List<KeyValue> result = new ArrayList<KeyValue>();
    this.store.get(get, qualifiers, result);
    assertEquals(value + amount, Bytes.toLong(result.get(0).getValue()));
  }

  /**
   * Same as above but for a negative number
   * @throws IOException
   */
  public void testIncrementColumnValue_UpdatingInPlace_Negative() 
  throws IOException {
    init(this.getName());

    //Put data in memstore
    long value = 3L;
    long amount = -1L;
    this.store.add(new KeyValue(row, family, qf1, Bytes.toBytes(value)));
    
    Store.ICVResult vas = this.store.incrementColumnValue(row, family, qf1, amount);
    assertEquals(vas.value, value+amount);
    store.add(vas.kv);
    Get get = new Get(row);
    get.addColumn(family, qf1);
    NavigableSet<byte[]> qualifiers = 
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qf1);
    List<KeyValue> result = new ArrayList<KeyValue>();
    this.store.get(get, qualifiers, result);
    assertEquals(value + amount, Bytes.toLong(result.get(0).getValue()));
  }
  
  /**
   * When there is no mathing key already, adding a new.
   * @throws IOException
   */
  public void testIncrementColumnValue_AddingNew() throws IOException {
    init(this.getName());
    
    //Put data in memstore
    long value = 1L;
    long amount = 3L;
    this.store.add(new KeyValue(row, family, qf1, Bytes.toBytes(value)));
    this.store.add(new KeyValue(row, family, qf2, Bytes.toBytes(value)));
    
    Store.ICVResult vas = this.store.incrementColumnValue(row, family, qf3, amount);
    store.add(vas.kv);
    Get get = new Get(row);
    get.addColumn(family, qf3);
    NavigableSet<byte[]> qualifiers = 
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qf3);
    List<KeyValue> result = new ArrayList<KeyValue>();
    this.store.get(get, qualifiers, result);
    assertEquals(amount, Bytes.toLong(result.get(0).getValue()));
  }

  /**
   * When we have the key in a file add a new key + value to memstore with the 
   * updates value. 
   * @throws IOException
   */
  public void testIncrementColumnValue_UpdatingFromSF() throws IOException {
    init(this.getName());
    
    //Put data in memstore
    long value = 1L;
    long amount = 3L;
    this.store.add(new KeyValue(row, family, qf1, Bytes.toBytes(value)));
    this.store.add(new KeyValue(row, family, qf2, Bytes.toBytes(value)));
    
    flush(1);
    
    Store.ICVResult vas = this.store.incrementColumnValue(row, family, qf1, amount);
    store.add(vas.kv);
    Get get = new Get(row);
    get.addColumn(family, qf1);
    NavigableSet<byte[]> qualifiers = 
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qf1);
    List<KeyValue> result = new ArrayList<KeyValue>();
    this.store.get(get, qualifiers, result);
    assertEquals(value + amount, Bytes.toLong(result.get(0).getValue()));
  }

  /**
   * Same as testIncrementColumnValue_AddingNew() except that the keys are
   * checked in file not in memstore
   * @throws IOException
   */
  public void testIncrementColumnValue_AddingNewAfterSFCheck() 
  throws IOException {
    init(this.getName());
    
    //Put data in memstore
    long value = 1L;
    long amount = 3L;
    this.store.add(new KeyValue(row, family, qf1, Bytes.toBytes(value)));
    this.store.add(new KeyValue(row, family, qf2, Bytes.toBytes(value)));
    
    flush(1);
    
    Store.ICVResult vas = this.store.incrementColumnValue(row, family, qf3, amount);
    store.add(vas.kv);
    Get get = new Get(row);
    get.addColumn(family, qf3);
    NavigableSet<byte[]> qualifiers = 
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    qualifiers.add(qf3);
    List<KeyValue> result = new ArrayList<KeyValue>();
    this.store.get(get, qualifiers, result);
    assertEquals(amount, Bytes.toLong(result.get(0).getValue()));
  }
  
}
