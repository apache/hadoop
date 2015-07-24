package org.apache.hadoop.io;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.channels.NonReadableChannelException;

class NewInputStream extends InputStream {

  File file = new File("C:\\Users\\mingleiz\\zhangminglei.txt");

  public NewInputStream() {}

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return super.read(b, off, len);
  }

  @Override
  public int read() throws IOException {
    return 0;
  }

  public void testNewInputStream() throws IOException{
    NewInputStream newInputStream = new NewInputStream();

    long startTime = System.currentTimeMillis();
    byte[] bytes = new byte[309000000];
    newInputStream.read(bytes, 0, 309000000);
    long endTime = System.currentTimeMillis();

    System.out.println("NewFileInputStream : =  " + (endTime - startTime));
  }

}

public class ComparePerformanceBetween_AFIS_FIS {

  File file = new File("C:\\Users\\mingleiz\\zhangminglei.txt");

  public long testFIS() throws URISyntaxException, IOException {
    long startTime = System.currentTimeMillis();
    FileInputStream fileInputStream = null;
    for (int i = 0; i < 100000; i++) {
      fileInputStream = new FileInputStream(file);
    }
    long endTime = System.currentTimeMillis();
    fileInputStream.close();
    System.out.println("FIS Consume time is = " + (endTime - startTime));
    return (endTime - startTime);
  }
  public long testAFIS() throws IOException, URISyntaxException, NonReadableChannelException {
    long startTime = System.currentTimeMillis();
    AltFileInputStream altFileInputStream;
    for (int i = 0; i < 100000; i++) {
      altFileInputStream = new AltFileInputStream(file);
    }
    long endTime = System.currentTimeMillis();
    System.out.println("AFIS Consume time is = " + (endTime - startTime));
    return (endTime - startTime);
  }

  public void testReadFromAFIS() throws Exception{
    AltFileInputStream altFileInputStream = new AltFileInputStream(file);

    long startTime = System.currentTimeMillis();
    byte[] bytes = new byte[309000000];
    altFileInputStream.read(bytes, 0, 309000000);
    long endTime = System.currentTimeMillis();

    System.out.println("AFIS ====   " + (endTime - startTime));
  }

  public void testReadFromFIS() throws Exception{

    long startTime = System.currentTimeMillis();
    FileInputStream fileInputStream = new FileInputStream(file);
    byte[] bytes = new byte[309000000];
    fileInputStream.read(bytes, 0, 309000000);
    long endTime = System.currentTimeMillis();

    System.out.println("FIS ====   " + (endTime - startTime));
  }

  public static void main(String[] args) throws Exception {
    ComparePerformanceBetween_AFIS_FIS cp = new ComparePerformanceBetween_AFIS_FIS();
    cp.testReadFromFIS();
    cp.testReadFromAFIS();
  }
}
