package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;

public class TestFileExpirationPolicy extends TestCase {

  public void testExpiration() {
    ChukwaAgent agent = null;

    try {
      agent = new ChukwaAgent();
      // Remove any adaptor left over from previous run
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      cli.removeAll();
      // sleep for some time to make sure we don't get chunk from existing
      // streams
      Thread.sleep(5000);

      FileTailingAdaptor.GRACEFUL_PERIOD = 30 * 1000;

      long adaptorId = agent
          .processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped MyType 0 /myWrongPath"
              + System.currentTimeMillis() + " 0");

      assertTrue(adaptorId != -1);

      assertTrue(agent.getAdaptorList().containsKey(adaptorId) == true);

      Thread.sleep(FileTailingAdaptor.GRACEFUL_PERIOD + 10000);
      assertTrue(agent.getAdaptorList().containsKey(adaptorId) == false);

    } catch (Exception e) {
      Assert.fail("Exception in TestFileExpirationPolicy");
    } finally {
      if (agent != null) {
        agent.shutdown();
      }
    }

  }

  public void testExpirationOnFileThatHasBennDeleted() {
    ChukwaAgent agent = null;
    File testFile = null;
    try {

      File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
      if (!tempDir.exists()) {
        tempDir.mkdirs();
      }
      String logFile = tempDir.getPath() + "/chukwatestExpiration.txt";
      testFile = makeTestFile(logFile, 8000);

      agent = new ChukwaAgent();
      // Remove any adaptor left over from previous run
      ChukwaConfiguration cc = new ChukwaConfiguration();
      int portno = cc.getInt("chukwaAgent.control.port", 9093);
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);
      cli.removeAll();
      // sleep for some time to make sure we don't get chunk from existing
      // streams
      Thread.sleep(5000);

      assertTrue(testFile.canRead() == true);

      FileTailingAdaptor.GRACEFUL_PERIOD = 30 * 1000;
      long adaptorId = agent
          .processCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped MyType 0 " 
              + logFile +" 0");

      assertTrue(adaptorId != -1);

      assertTrue(agent.getAdaptorList().containsKey(adaptorId) == true);

      Thread.sleep(10000);
      testFile.delete();

      Thread.sleep(FileTailingAdaptor.GRACEFUL_PERIOD + 10000);
      assertTrue(agent.getAdaptorList().containsKey(adaptorId) == false);
      agent.shutdown();
    } catch (Exception e) {
      Assert.fail("Exception in TestFileExpirationPolicy");
    } finally {
      if (agent != null) {
        agent.shutdown();
      }
    }
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
