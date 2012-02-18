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

package org.apache.hadoop.lib.servlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.server.ServerException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.text.MessageFormat;

/**
 * {@link Server} subclass that implements <code>ServletContextListener</code>
 * and uses its lifecycle to start and stop the server.
 */
public abstract class ServerWebApp extends Server implements ServletContextListener {

  private static final String HOME_DIR = ".home.dir";
  private static final String CONFIG_DIR = ".config.dir";
  private static final String LOG_DIR = ".log.dir";
  private static final String TEMP_DIR = ".temp.dir";

  private static ThreadLocal<String> HOME_DIR_TL = new ThreadLocal<String>();

  /**
   * Method for testing purposes.
   */
  public static void setHomeDirForCurrentThread(String homeDir) {
    HOME_DIR_TL.set(homeDir);
  }

  /**
   * Constructor for testing purposes.
   */
  protected ServerWebApp(String name, String homeDir, String configDir, String logDir, String tempDir,
                         Configuration config) {
    super(name, homeDir, configDir, logDir, tempDir, config);
  }

  /**
   * Constructor for testing purposes.
   */
  protected ServerWebApp(String name, String homeDir, Configuration config) {
    super(name, homeDir, config);
  }

  /**
   * Constructor. Subclasses must have a default constructor specifying
   * the server name.
   * <p/>
   * The server name is used to resolve the Java System properties that define
   * the server home, config, log and temp directories.
   * <p/>
   * The home directory is looked in the Java System property
   * <code>#SERVER_NAME#.home.dir</code>.
   * <p/>
   * The config directory is looked in the Java System property
   * <code>#SERVER_NAME#.config.dir</code>, if not defined it resolves to
   * the <code>#SERVER_HOME_DIR#/conf</code> directory.
   * <p/>
   * The log directory is looked in the Java System property
   * <code>#SERVER_NAME#.log.dir</code>, if not defined it resolves to
   * the <code>#SERVER_HOME_DIR#/log</code> directory.
   * <p/>
   * The temp directory is looked in the Java System property
   * <code>#SERVER_NAME#.temp.dir</code>, if not defined it resolves to
   * the <code>#SERVER_HOME_DIR#/temp</code> directory.
   *
   * @param name server name.
   */
  public ServerWebApp(String name) {
    super(name, getHomeDir(name),
          getDir(name, CONFIG_DIR, getHomeDir(name) + "/conf"),
          getDir(name, LOG_DIR, getHomeDir(name) + "/log"),
          getDir(name, TEMP_DIR, getHomeDir(name) + "/temp"), null);
  }

  /**
   * Returns the server home directory.
   * <p/>
   * It is looked up in the Java System property
   * <code>#SERVER_NAME#.home.dir</code>.
   *
   * @param name the server home directory.
   *
   * @return the server home directory.
   */
  static String getHomeDir(String name) {
    String homeDir = HOME_DIR_TL.get();
    if (homeDir == null) {
      String sysProp = name + HOME_DIR;
      homeDir = System.getProperty(sysProp);
      if (homeDir == null) {
        throw new IllegalArgumentException(MessageFormat.format("System property [{0}] not defined", sysProp));
      }
    }
    return homeDir;
  }

  /**
   * Convenience method that looks for Java System property defining a
   * diretory and if not present defaults to the specified directory.
   *
   * @param name server name, used as prefix of the Java System property.
   * @param dirType dir type, use as postfix of the Java System property.
   * @param defaultDir the default directory to return if the Java System
   * property <code>name + dirType</code> is not defined.
   *
   * @return the directory defined in the Java System property or the
   *         the default directory if the Java System property is not defined.
   */
  static String getDir(String name, String dirType, String defaultDir) {
    String sysProp = name + dirType;
    return System.getProperty(sysProp, defaultDir);
  }

  /**
   * Initializes the <code>ServletContextListener</code> which initializes
   * the Server.
   *
   * @param event servelt context event.
   */
  public void contextInitialized(ServletContextEvent event) {
    try {
      init();
    } catch (ServerException ex) {
      event.getServletContext().log("ERROR: " + ex.getMessage());
      throw new RuntimeException(ex);
    }
  }

  /**
   * Destroys the <code>ServletContextListener</code> which destroys
   * the Server.
   *
   * @param event servelt context event.
   */
  public void contextDestroyed(ServletContextEvent event) {
    destroy();
  }

}
