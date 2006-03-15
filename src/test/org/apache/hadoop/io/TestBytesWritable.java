package org.apache.hadoop.io;

import junit.framework.TestCase;

/**
 * This is the unit test for BytesWritable.
 * @author Owen O'Malley
 */
public class TestBytesWritable extends TestCase {

  public void testSizeChange() throws Exception {
    byte[] hadoop = "hadoop".getBytes();
    BytesWritable buf = new BytesWritable(hadoop);
    int size = buf.getSize();
    int orig_capacity = buf.getCapacity();
    buf.setSize(size*2);
    int new_capacity = buf.getCapacity();
    System.arraycopy(buf.get(),0, buf.get(), size, size);
    assertTrue(new_capacity >= size * 2);
    assertEquals(size * 2, buf.getSize());
    assertTrue(new_capacity != orig_capacity);
    buf.setSize(size*4);
    assertTrue(new_capacity != buf.getCapacity());
    for(int i=0; i < size*2; ++i) {
      assertEquals(hadoop[i%size], buf.get()[i]);
    }
    // shrink the buffer
    buf.setCapacity(1);
    // make sure the size has been cut down too
    assertEquals(1, buf.getSize());
    // but that the data is still there
    assertEquals(hadoop[0], buf.get()[0]);
  }
  
  public void testHash() throws Exception {
    byte[] owen = "owen".getBytes();
    BytesWritable buf = new BytesWritable(owen);
    assertEquals(4347922, buf.hashCode());
    buf.setCapacity(10000);
    assertEquals(4347922, buf.hashCode());
    buf.setSize(0);
    assertEquals(1, buf.hashCode());
  }
  
  public void testCompare() throws Exception {
    byte[][] values = new byte[][]{"abc".getBytes(), 
        "ad".getBytes(),
        "abcd".getBytes(),
        "".getBytes(),
        "b".getBytes()};
    BytesWritable[] buf = new BytesWritable[values.length];
    for(int i=0; i < values.length; ++i) {
      buf[i] = new BytesWritable(values[i]);
    }
    // check to make sure the compare function is symetric and reflexive
    for(int i=0; i < values.length; ++i) {
      for(int j=0; j < values.length; ++j) {
        assertTrue(buf[i].compareTo(buf[j]) == -buf[j].compareTo(buf[i]));
        assertTrue((i == j) == (buf[i].compareTo(buf[j]) == 0));
      }
    }
    assertTrue(buf[0].compareTo(buf[1]) < 0);
    assertTrue(buf[1].compareTo(buf[2]) > 0);
    assertTrue(buf[2].compareTo(buf[3]) > 0);
    assertTrue(buf[3].compareTo(buf[4]) < 0);
  }
}

