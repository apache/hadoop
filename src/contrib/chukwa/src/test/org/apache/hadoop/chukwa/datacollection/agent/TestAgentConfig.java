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
  
  
  public void testNoCheckpoints() {
    try {
      String tmpdir = System.getProperty("test.build.data", "/tmp");
      File NONCE_DIR = new File(tmpdir,"/test_chukwa_checkpoints");
      if(NONCE_DIR.exists()) {
        for(File f: NONCE_DIR.listFiles())
          f.delete();
        NONCE_DIR.delete();
      }
//      assertFalse(NONCE_DIR.exists());
      Configuration conf = new Configuration();
      conf.set("chukwaAgent.checkpoint.dir", NONCE_DIR.getAbsolutePath());
      conf.setBoolean("chukwaAgent.checkpoint.enabled", true);
      conf.setInt("chukwaAgent.control.port", 0);
      
      System.out.println("\n\n===checkpoints enabled, dir does not exist:");
      ChukwaAgent agent = new ChukwaAgent(conf);
      assertEquals(0, agent.getAdaptorList().size());
      agent.shutdown();
      assertTrue(NONCE_DIR.exists());
      for(File f: NONCE_DIR.listFiles())
        f.delete();

      System.out.println("\n\n===checkpoints enabled, dir exists but is empty:");
      agent = new ChukwaAgent(conf);
      assertEquals(0, agent.getAdaptorList().size());
      agent.shutdown();
      for(File f: NONCE_DIR.listFiles())
        f.delete();
      
      System.out.println("\n\n===checkpoints enabled, dir exists with zero-length file:");
      (new File(NONCE_DIR, "chukwa_checkpoint_0")).createNewFile();
      agent = new ChukwaAgent(conf);
      assertEquals(0, agent.getAdaptorList().size());
      agent.processCommand("ADD org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor testdata  0");
      agent.shutdown();
      assertTrue(new File(NONCE_DIR, "chukwa_checkpoint_1").exists());

      System.out.println("\n\n===checkpoints enabled, dir exists with valid checkpoint");
      agent = new ChukwaAgent(conf);
      assertEquals(1, agent.getAdaptorList().size());
      agent.shutdown();
      //checkpoint # increments by one on boot and reload
      assertTrue(new File(NONCE_DIR, "chukwa_checkpoint_2").exists());

      
    }
    catch(Exception e) {
      fail(e.toString());
    }
  }
  
  
  
}
