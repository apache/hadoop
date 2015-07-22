package org.apache.hadoop.io;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.*;

public class TestAltFileInputStream extends TestCase {

  File file = new File("C:\\Users\\mingleiz\\test.txt");

  @Test
  public void testGetFD() throws Exception {

      AltFileInputStream afls = new AltFileInputStream(file);
      assertNotNull(afls.getFD());

      AltFileInputStream afls_ = new AltFileInputStream(afls.getFD(),afls.getChannel());
      assertNotNull(afls_.getFD());

  }

  @Test
  public void testGetChannel() throws Exception {
    AltFileInputStream afls = new AltFileInputStream(file);
    assertNotNull(afls.getChannel());

    AltFileInputStream afls_ = new AltFileInputStream(afls.getFD(),afls.getChannel());
    assertNotNull(afls_.getChannel());
  }
  @Test
  public void testRead() throws Exception {
    InputStream inputStream = new FileInputStream(file);
    long numberIS = 0;
    while (inputStream.read() != -1){
      numberIS ++;
    }
    AltFileInputStream alfs = new AltFileInputStream(file);
    long numberASIS = 0;
    while ( alfs.read() != -1 ){
      numberASIS ++;
    }
    assertEquals(numberASIS, numberIS);
  }
  @Test
  public void testClose() throws Exception {
    AltFileInputStream afls = new AltFileInputStream(file);
    afls.close();
    try {
      afls.read();
    } catch (IOException ex){
      assert (ex.getMessage().equals("Stream Closed"));
      return;
    }
    throw new Exception("close failure");
  }
}