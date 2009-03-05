package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestFileTailingAdaptorBigRecord extends TestCase {

  ChunkCatcherConnector chunks;

  public void testBigRecord() {
    File f = null;
    try {
      File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
      if (!tempDir.exists()) {
        tempDir.mkdirs();
      }
      String logFile = tempDir.getPath() + "/Chukwa-bigRecord.txt";
      f = makeTestFile(logFile);

      chunks = new ChunkCatcherConnector();
      chunks.start();

      // Remove any adaptor left over from previous run
      ChukwaConfiguration cc = new ChukwaConfiguration();
      cc.set("chukwaAgent.control.port", "0");
      cc.setInt("chukwaAgent.fileTailingAdaptor.maxReadSize", 55);
      ChukwaAgent agent = new ChukwaAgent(cc);
      int portno = agent.getControllerPort();
      while (portno == -1) {
        Thread.sleep(1000);
        portno = agent.getControllerPort();
      }

      // System.out.println("Port number:" + portno);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      cli.removeAll();
      // sleep for some time to make sure we don't get chunk from existing
      // streams
      Thread.sleep(5000);
      long adaptorId = agent
          .processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped"
              + " BigRecord " + logFile + " 0");
      assertTrue(adaptorId != -1);

      boolean record8Found = false;
      Chunk c = null;
      // Keep reading until record8
      // If the adaptor is stopped then Junit will fail with a timeOut
      while (!record8Found) {
        c = chunks.waitForAChunk();
        String data = new String(c.getData());
        if (c.getDataType().equals("BigRecord")
            && data.indexOf("8 abcdefghijklmnopqrstuvwxyz") >= 0) {
          record8Found = true;
        }
      }
      agent.getAdaptorList().get(adaptorId).shutdown();
      agent.shutdown();
    } catch (Exception e) {
      Assert.fail("Exception in testBigRecord" + e.getMessage());
    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  private File makeTestFile(String name) throws IOException {
    File tmpOutput = new File(name);
    FileOutputStream fos = new FileOutputStream(tmpOutput);

    PrintWriter pw = new PrintWriter(fos);
    for (int i = 0; i < 5; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.print("6 ");
    for (int i = 0; i < 10; ++i) {
      pw.print("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
    }
    pw.print("\n");
    pw.print("7 ");
    pw.println("abcdefghijklmnopqrstuvwxyz");
    pw.print("8 ");
    pw.println("abcdefghijklmnopqrstuvwxyz");

    pw.flush();
    pw.close();
    return tmpOutput;
  }

}
