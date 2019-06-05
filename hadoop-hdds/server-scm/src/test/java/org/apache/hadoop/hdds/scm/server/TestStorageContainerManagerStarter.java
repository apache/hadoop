package org.apache.hadoop.hdds.scm.server;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.junit.Assert.*;

public class TestStorageContainerManagerStarter {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;

  private MockSCMStarter mock;

  @Before
  public void setUpStreams() {
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
    mock = new MockSCMStarter();
  }

  @After
  public void restoreStreams() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void TestCallsStartWhenServerStarted() throws Exception {
    executeCommand();
    assertTrue(mock.startCalled);
  }

  @Test
  public void TestExceptionThrownWhenStartFails() throws Exception {
    mock.throwOnStart = true;
    try {
      executeCommand();
      fail("Exception show have been thrown");
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void TestStartNotCalledWithInvalidParam() throws Exception {
    executeCommand("--invalid");
    assertFalse(mock.startCalled);
  }

  @Test
  public void TestPassingInitSwitchCallsInit() {
    executeCommand("--init");
    assertTrue(mock.initCalled);
  }

  @Test
  public void TestInitSwitchAcceptsClusterIdSSwitch() {
    executeCommand("--init", "--clusterid=abcdefg");
    assertEquals("abcdefg", mock.clusterId);
  }

  @Test
  public void TestInitSwitchWithInvalidParamDoesNotRun() {
    executeCommand("--init", "--clusterid=abcdefg", "--invalid" );
    assertFalse(mock.initCalled);
  }

  @Test
  public void TestUnSuccessfulInitThrowsException() {
    mock.throwOnInit = true;
      try {
        executeCommand("--init");
      fail("Exception show have been thrown");
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void TestGenClusterIdRunsGenerate() {
    executeCommand("--genclusterid");
    assertTrue(mock.generateCalled);
  }

  @Test
  public void TestGenClusterIdWithInvalidParamDoesNotRun() {
    executeCommand("--genclusterid", "--invalid");
    assertFalse(mock.generateCalled);
  }

  @Test
  public void TestUsagePrintedOnInvalidInput() {
    executeCommand("--invalid");
    Pattern p = Pattern.compile("^Unknown option:.*--invalid.*\nUsage");
    Matcher m = p.matcher(errContent.toString());
    assertTrue(m.find());
  }

  private void executeCommand(String ... args) {
    new StorageContainerManagerStarter(mock).execute(args);
  }

  static class MockSCMStarter implements SCMStarterInterface {

    public boolean initStatus = true;
    public boolean throwOnStart = false;
    public boolean throwOnInit  = false;
    public boolean startCalled = false;
    public boolean initCalled = false;
    public boolean generateCalled = false;
    public String clusterId = null;

    public void start(OzoneConfiguration conf) throws Exception {
      if (throwOnStart) {
        throw new Exception("Simulated error on start");
      }
      startCalled = true;
    }

    public boolean init(OzoneConfiguration conf, String cid)
        throws IOException {
      if (throwOnInit) {
        throw new IOException("Simulated error on init");
      }
      initCalled = true;
      clusterId = cid;
      return initStatus;
    }

    public String generateClusterId() {
      generateCalled = true;
      return "static-cluster-id";
    }
  }
}