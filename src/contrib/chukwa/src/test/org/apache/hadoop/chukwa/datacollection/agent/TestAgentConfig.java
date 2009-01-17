package org.apache.hadoop.chukwa.datacollection.agent;

import java.io.*;

import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

public class TestAgentConfig extends TestCase {
  public void testInitAdaptors_vs_Checkpoint() {
    try {
        //create two target files, foo and bar
      File foo = File.createTempFile("foo", "test");
      foo.deleteOnExit();
      PrintStream ps = new PrintStream(new FileOutputStream(foo));
      ps.println("foo");
      ps.close();

      File bar = File.createTempFile("bar", "test");
      bar.deleteOnExit();
      ps = new PrintStream(new FileOutputStream(bar));
      ps.println("bar");
      ps.close();
      
        //initially, read foo
      File initialAdaptors = File.createTempFile("initial", "adaptors");
      initialAdaptors.deleteOnExit();
      ps = new PrintStream(new FileOutputStream(initialAdaptors));
      ps.println("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8  raw 0 "
          + foo.getAbsolutePath() +" 0  ");
      ps.close();
      
      Configuration conf = new Configuration();
      conf.set("chukwaAgent.initial_adaptors", initialAdaptors.getAbsolutePath());
      File checkpointDir = File.createTempFile("chukwatest", "checkpoint");
      checkpointDir.delete();
      checkpointDir.mkdir();
      checkpointDir.deleteOnExit();
      conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getAbsolutePath());
      
      ChukwaAgent agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      assertEquals(1, agent.adaptorCount());//check that we processed initial adaptors
      assertNotNull(agent.getAdaptorList().get(1L));
      assertTrue(agent.getAdaptorList().get(1L).getStreamName().contains("foo"));
      
      System.out.println("---------------------done with first run, now stopping");
      agent.shutdown();
      assertEquals(0, agent.adaptorCount());
      //at this point, there should be a checkpoint file with a tailer reading foo.
      //we're going to rewrite initial adaptors to read bar; but after reboot we should
      //still only be looking at foo.
      ps = new PrintStream(new FileOutputStream(initialAdaptors, false));//overwrite
      ps.println("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8  raw 0 "
          + bar.getAbsolutePath() +" 0  ");
      ps.close();

      System.out.println("---------------------restarting");
      agent = new ChukwaAgent(conf);
      conn = new ConsoleOutConnector(agent, true);
      conn.start();
      assertEquals(1, agent.adaptorCount());//check that we processed initial adaptors
      assertNotNull(agent.getAdaptorList().get(1L));
      assertTrue(agent.getAdaptorList().get(1L).getStreamName().contains("foo"));
      agent.shutdown();
      System.out.println("---------------------done");
      
      
    } catch(Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
}
