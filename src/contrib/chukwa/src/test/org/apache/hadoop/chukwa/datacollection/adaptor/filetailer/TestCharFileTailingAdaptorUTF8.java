package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;

public class TestCharFileTailingAdaptorUTF8 extends TestCase {
  ChunkCatcherConnector chunks;

  public TestCharFileTailingAdaptorUTF8() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }

  public void testCrSepAdaptor() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    ChukwaAgent agent = new ChukwaAgent();
    File testFile = makeTestFile("/tmp/chukwaTest", 80);
    long adaptorId = agent
        .processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8"
            + " lines " + testFile + " 0");
    assertTrue(adaptorId != -1);
    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk();
    System.out.println("got chunk");
    assertTrue(c.getSeqID() == testFile.length());

    assertTrue(c.getRecordOffsets().length == 80);
    int recStart = 0;
    for (int rec = 0; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart,
          c.getRecordOffsets()[rec] - recStart + 1);
      System.out.println("record " + rec + " was: " + record);
      assertTrue(record.equals(rec + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] + 1;
    }
    assertTrue(c.getDataType().equals("lines"));
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
  }

  private File makeTestFile(String name, int size) throws IOException {
    File tmpOutput = new File(name);
    FileOutputStream fos = new FileOutputStream(tmpOutput);

    PrintWriter pw = new PrintWriter(fos);
    for (int i = 0; i < size; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    return tmpOutput;
  }

}
