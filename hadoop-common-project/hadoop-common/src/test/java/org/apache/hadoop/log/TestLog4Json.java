/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.spi.HierarchyEventListener;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.ThrowableInformation;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.NoRouteToHostException;
import java.util.Enumeration;
import java.util.Vector;

public class TestLog4Json extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestLog4Json.class);

  @Test
  public void testConstruction() throws Throwable {
    Log4Json l4j = new Log4Json();
    String outcome = l4j.toJson(new StringWriter(),
        "name", 0, "DEBUG", "thread1",
        "hello, world", null).toString();
    println("testConstruction", outcome);
  }

  @Test
  public void testException() throws Throwable {
    Exception e =
        new NoRouteToHostException("that box caught fire 3 years ago");
    ThrowableInformation ti = new ThrowableInformation(e);
    Log4Json l4j = new Log4Json();
    long timeStamp = Time.now();
    String outcome = l4j.toJson(new StringWriter(),
        "testException",
        timeStamp,
        "INFO",
        "quoted\"",
        "new line\n and {}",
        ti)
        .toString();
    println("testException", outcome);
  }

  @Test
  public void testNestedException() throws Throwable {
    Exception e =
        new NoRouteToHostException("that box caught fire 3 years ago");
    Exception ioe = new IOException("Datacenter problems", e);
    ThrowableInformation ti = new ThrowableInformation(ioe);
    Log4Json l4j = new Log4Json();
    long timeStamp = Time.now();
    String outcome = l4j.toJson(new StringWriter(),
        "testNestedException",
        timeStamp,
        "INFO",
        "quoted\"",
        "new line\n and {}",
        ti)
        .toString();
    println("testNestedException", outcome);
    ContainerNode rootNode = Log4Json.parse(outcome);
    assertEntryEquals(rootNode, Log4Json.LEVEL, "INFO");
    assertEntryEquals(rootNode, Log4Json.NAME, "testNestedException");
    assertEntryEquals(rootNode, Log4Json.TIME, timeStamp);
    assertEntryEquals(rootNode, Log4Json.EXCEPTION_CLASS,
        ioe.getClass().getName());
    JsonNode node = assertNodeContains(rootNode, Log4Json.STACK);
    assertTrue("Not an array: " + node, node.isArray());
    node = assertNodeContains(rootNode, Log4Json.DATE);
    assertTrue("Not a string: " + node, node.isTextual());
    //rather than try and make assertions about the format of the text
    //message equalling another ISO date, this test asserts that the hypen
    //and colon characters are in the string.
    String dateText = node.textValue();
    assertTrue("No '-' in " + dateText, dateText.contains("-"));
    assertTrue("No '-' in " + dateText, dateText.contains(":"));

  }


  /**
   * Create a log instance and and log to it
   * @throws Throwable if it all goes wrong
   */
  @Test
  public void testLog() throws Throwable {
    String message = "test message";
    Throwable throwable = null;
    String json = logOut(message, throwable);
    println("testLog", json);
  }

  /**
   * Create a log instance and and log to it
   * @throws Throwable if it all goes wrong
   */
  @Test
  public void testLogExceptions() throws Throwable {
    String message = "test message";
    Throwable inner = new IOException("Directory / not found");
    Throwable throwable = new IOException("startup failure", inner);
    String json = logOut(message, throwable);
    println("testLogExceptions", json);
  }


  void assertEntryEquals(ContainerNode rootNode, String key, String value) {
    JsonNode node = assertNodeContains(rootNode, key);
    assertEquals(value, node.textValue());
  }

  private JsonNode assertNodeContains(ContainerNode rootNode, String key) {
    JsonNode node = rootNode.get(key);
    if (node == null) {
      fail("No entry of name \"" + key + "\" found in " + rootNode.toString());
    }
    return node;
  }

  void assertEntryEquals(ContainerNode rootNode, String key, long value) {
    JsonNode node = assertNodeContains(rootNode, key);
    assertEquals(value, node.numberValue());
  }

  /**
   * Print out what's going on. The logging APIs aren't used and the text
   * delimited for more details
   *
   * @param name name of operation
   * @param text text to print
   */
  private void println(String name, String text) {
    System.out.println(name + ": #" + text + "#");
  }

  private String logOut(String message, Throwable throwable) {
    StringWriter writer = new StringWriter();
    Logger logger = createLogger(writer);
    logger.info(message, throwable);
    //remove and close the appender
    logger.removeAllAppenders();
    return writer.toString();
  }

  public Logger createLogger(Writer writer) {
    TestLoggerRepository repo = new TestLoggerRepository();
    Logger logger = repo.getLogger("test");
    Log4Json layout = new Log4Json();
    WriterAppender appender = new WriterAppender(layout, writer);
    logger.addAppender(appender);
    return logger;
  }

  /**
   * This test logger avoids integrating with the main runtimes Logger hierarchy
   * in ways the reader does not want to know.
   */
  private static class TestLogger extends Logger {
    private TestLogger(String name, LoggerRepository repo) {
      super(name);
      repository = repo;
      setLevel(Level.INFO);
    }

  }

  public static class TestLoggerRepository implements LoggerRepository {
    @Override
    public void addHierarchyEventListener(HierarchyEventListener listener) {
    }

    @Override
    public boolean isDisabled(int level) {
      return false;
    }

    @Override
    public void setThreshold(Level level) {
    }

    @Override
    public void setThreshold(String val) {
    }

    @Override
    public void emitNoAppenderWarning(Category cat) {
    }

    @Override
    public Level getThreshold() {
      return Level.ALL;
    }

    @Override
    public Logger getLogger(String name) {
      return new TestLogger(name, this);
    }

    @Override
    public Logger getLogger(String name, LoggerFactory factory) {
      return new TestLogger(name, this);
    }

    @Override
    public Logger getRootLogger() {
      return new TestLogger("root", this);
    }

    @Override
    public Logger exists(String name) {
      return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Enumeration getCurrentLoggers() {
      return new Vector().elements();
    }

    @Override
    public Enumeration getCurrentCategories() {
      return new Vector().elements();
    }

    @Override
    public void fireAddAppenderEvent(Category logger, Appender appender) {
    }

    @Override
    public void resetConfiguration() {
    }
  }
}
