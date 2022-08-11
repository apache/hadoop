package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetJournalEditServlet {

  private final static Configuration conf = new HdfsConfiguration();

  private final static GetJournalEditServlet servlet = new GetJournalEditServlet();

  @BeforeClass
  public static void setUp() throws ServletException {
    LogManager.getLogger(GetJournalEditServlet.class).setLevel(Level.DEBUG);

    // Configure Hadoop
    conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, "hdfs://localhost:4321/");
    conf.set(DFSConfigKeys.HADOOP_SECURITY_AUTH_TO_LOCAL,
        "RULE:[2:$1/$2@$0]([nsdj]n/.*@REALM\\.TLD)s/.*/hdfs/\nDEFAULT");
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns");
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, "nn/_HOST@REALM.TLD");

    // Configure Kerberos UGI
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser(
        "jn/somehost@REALM.TLD"));

    // Initialize the servlet
    ServletConfig config = mock(ServletConfig.class);
    servlet.init(config);
  }

  /**
   * Unauthenticated user should be rejected.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testWithoutUser() throws IOException {
    // Test: Make a request without specifying a user
    HttpServletRequest request = mock(HttpServletRequest.class);
    boolean isValid = servlet.isValidRequestor(request, conf);

    // Verify: The request is invalid
    assertThat(isValid).isFalse();
  }

  /**
   * Namenode requests should be authorized, since it will match the configured namenode.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testRequestNameNode() throws IOException, ServletException {
    // Test: Make a request from a namenode
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter(UserParam.NAME)).thenReturn("nn/localhost@REALM.TLD");
    boolean isValid = servlet.isValidRequestor(request, conf);

    assertThat(isValid).isTrue();
  }

  /**
   * There is a fallback using the short name, which is used by journalnodes.
   *
   * @throws IOException for unexpected validation failures
   */
  @Test
  public void testRequestShortName() throws IOException {
    // Test: Make a request from a namenode
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getParameter(UserParam.NAME)).thenReturn("jn/localhost@REALM.TLD");
    boolean isValid = servlet.isValidRequestor(request, conf);

    assertThat(isValid).isTrue();
  }

}