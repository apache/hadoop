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

package org.apache.hadoop.lib.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.lib.util.Check;
import org.apache.hadoop.lib.util.ConfigurationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Server class provides standard configuration, logging and {@link Service}
 * lifecyle management.
 * <p/>
 * A Server normally has a home directory, a configuration directory, a temp
 * directory and logs directory.
 * <p/>
 * The Server configuration is loaded from 2 overlapped files,
 * <code>#SERVER#-default.xml</code> and <code>#SERVER#-site.xml</code>. The
 * default file is loaded from the classpath, the site file is laoded from the
 * configuration directory.
 * <p/>
 * The Server collects all configuration properties prefixed with
 * <code>#SERVER#</code>. The property names are then trimmed from the
 * <code>#SERVER#</code> prefix.
 * <p/>
 * The Server log configuration is loaded from the
 * <code>#SERVICE#-log4j.properties</code> file in the configuration directory.
 * <p/>
 * The lifecycle of server is defined in by {@link Server.Status} enum.
 * When a server is create, its status is UNDEF, when being initialized it is
 * BOOTING, once initialization is complete by default transitions to NORMAL.
 * The <code>#SERVER#.startup.status</code> configuration property can be used
 * to specify a different startup status (NORMAL, ADMIN or HALTED).
 * <p/>
 * Services classes are defined in the <code>#SERVER#.services</code> and
 * <code>#SERVER#.services.ext</code> properties. They are loaded in order
 * (services first, then services.ext).
 * <p/>
 * Before initializing the services, they are traversed and duplicate service
 * interface are removed from the service list. The last service using a given
 * interface wins (this enables a simple override mechanism).
 * <p/>
 * After the services have been resoloved by interface de-duplication they are
 * initialized in order. Once all services are initialized they are
 * post-initialized (this enables late/conditional service bindings).
 * <p/>
 */
@InterfaceAudience.Private
public class Server {
  private Logger log;

  /**
   * Server property name that defines the service classes.
   */
  public static final String CONF_SERVICES = "services";

  /**
   * Server property name that defines the service extension classes.
   */
  public static final String CONF_SERVICES_EXT = "services.ext";

  /**
   * Server property name that defines server startup status.
   */
  public static final String CONF_STARTUP_STATUS = "startup.status";

  /**
   * Enumeration that defines the server status.
   */
  @InterfaceAudience.Private
  public static enum Status {
    UNDEF(false, false),
    BOOTING(false, true),
    HALTED(true, true),
    ADMIN(true, true),
    NORMAL(true, true),
    SHUTTING_DOWN(false, true),
    SHUTDOWN(false, false);

    private boolean settable;
    private boolean operational;

    /**
     * Status constructor.
     *
     * @param settable indicates if the status is settable.
     * @param operational indicates if the server is operational
     * when in this status.
     */
    private Status(boolean settable, boolean operational) {
      this.settable = settable;
      this.operational = operational;
    }

    /**
     * Returns if this server status is operational.
     *
     * @return if this server status is operational.
     */
    public boolean isOperational() {
      return operational;
    }
  }

  /**
   * Name of the log4j configuration file the Server will load from the
   * classpath if the <code>#SERVER#-log4j.properties</code> is not defined
   * in the server configuration directory.
   */
  public static final String DEFAULT_LOG4J_PROPERTIES = "default-log4j.properties";

  private Status status;
  private String name;
  private String homeDir;
  private String configDir;
  private String logDir;
  private String tempDir;
  private Configuration config;
  private Map<Class, Service> services = new LinkedHashMap<Class, Service>();

  /**
   * Creates a server instance.
   * <p/>
   * The config, log and temp directories are all under the specified home directory.
   *
   * @param name server name.
   * @param homeDir server home directory.
   */
  public Server(String name, String homeDir) {
    this(name, homeDir, null);
  }

  /**
   * Creates a server instance.
   *
   * @param name server name.
   * @param homeDir server home directory.
   * @param configDir config directory.
   * @param logDir log directory.
   * @param tempDir temp directory.
   */
  public Server(String name, String homeDir, String configDir, String logDir, String tempDir) {
    this(name, homeDir, configDir, logDir, tempDir, null);
  }

  /**
   * Creates a server instance.
   * <p/>
   * The config, log and temp directories are all under the specified home directory.
   * <p/>
   * It uses the provided configuration instead loading it from the config dir.
   *
   * @param name server name.
   * @param homeDir server home directory.
   * @param config server configuration.
   */
  public Server(String name, String homeDir, Configuration config) {
    this(name, homeDir, homeDir + "/conf", homeDir + "/log", homeDir + "/temp", config);
  }

  /**
   * Creates a server instance.
   * <p/>
   * It uses the provided configuration instead loading it from the config dir.
   *
   * @param name server name.
   * @param homeDir server home directory.
   * @param configDir config directory.
   * @param logDir log directory.
   * @param tempDir temp directory.
   * @param config server configuration.
   */
  public Server(String name, String homeDir, String configDir, String logDir, String tempDir, Configuration config) {
    this.name = Check.notEmpty(name, "name").trim().toLowerCase();
    this.homeDir = Check.notEmpty(homeDir, "homeDir");
    this.configDir = Check.notEmpty(configDir, "configDir");
    this.logDir = Check.notEmpty(logDir, "logDir");
    this.tempDir = Check.notEmpty(tempDir, "tempDir");
    checkAbsolutePath(homeDir, "homeDir");
    checkAbsolutePath(configDir, "configDir");
    checkAbsolutePath(logDir, "logDir");
    checkAbsolutePath(tempDir, "tempDir");
    if (config != null) {
      this.config = new Configuration(false);
      ConfigurationUtils.copy(config, this.config);
    }
    status = Status.UNDEF;
  }

  /**
   * Validates that the specified value is an absolute path (starts with '/').
   *
   * @param value value to verify it is an absolute path.
   * @param name name to use in the exception if the value is not an absolute
   * path.
   *
   * @return the value.
   *
   * @throws IllegalArgumentException thrown if the value is not an absolute
   * path.
   */
  private String checkAbsolutePath(String value, String name) {
    if (!new File(value).isAbsolute()) {
      throw new IllegalArgumentException(
        MessageFormat.format("[{0}] must be an absolute path [{1}]", name, value));
    }
    return value;
  }

  /**
   * Returns the current server status.
   *
   * @return the current server status.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Sets a new server status.
   * <p/>
   * The status must be settable.
   * <p/>
   * All services will be notified o the status change via the
   * {@link Service#serverStatusChange(Server.Status, Server.Status)} method. If a service
   * throws an exception during the notification, the server will be destroyed.
   *
   * @param status status to set.
   *
   * @throws ServerException thrown if the service has been destroy because of
   * a failed notification to a service.
   */
  public void setStatus(Status status) throws ServerException {
    Check.notNull(status, "status");
    if (status.settable) {
      if (status != this.status) {
        Status oldStatus = this.status;
        this.status = status;
        for (Service service : services.values()) {
          try {
            service.serverStatusChange(oldStatus, status);
          } catch (Exception ex) {
            log.error("Service [{}] exception during status change to [{}] -server shutting down-,  {}",
                      new Object[]{service.getInterface().getSimpleName(), status, ex.getMessage(), ex});
            destroy();
            throw new ServerException(ServerException.ERROR.S11, service.getInterface().getSimpleName(),
                                      status, ex.getMessage(), ex);
          }
        }
      }
    } else {
      throw new IllegalArgumentException("Status [" + status + " is not settable");
    }
  }

  /**
   * Verifies the server is operational.
   *
   * @throws IllegalStateException thrown if the server is not operational.
   */
  protected void ensureOperational() {
    if (!getStatus().isOperational()) {
      throw new IllegalStateException("Server is not running");
    }
  }

  /**
   * Convenience method that returns a resource as inputstream from the
   * classpath.
   * <p/>
   * It first attempts to use the Thread's context classloader and if not
   * set it uses the <code>ClassUtils</code> classloader.
   *
   * @param name resource to retrieve.
   *
   * @return inputstream with the resource, NULL if the resource does not
   *         exist.
   */
  static InputStream getResource(String name) {
    Check.notEmpty(name, "name");
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = Server.class.getClassLoader();
    }
    return cl.getResourceAsStream(name);
  }

  /**
   * Initializes the Server.
   * <p/>
   * The initialization steps are:
   * <ul>
   * <li>It verifies the service home and temp directories exist</li>
   * <li>Loads the Server <code>#SERVER#-default.xml</code>
   * configuration file from the classpath</li>
   * <li>Initializes log4j logging. If the
   * <code>#SERVER#-log4j.properties</code> file does not exist in the config
   * directory it load <code>default-log4j.properties</code> from the classpath
   * </li>
   * <li>Loads the <code>#SERVER#-site.xml</code> file from the server config
   * directory and merges it with the default configuration.</li>
   * <li>Loads the services</li>
   * <li>Initializes the services</li>
   * <li>Post-initializes the services</li>
   * <li>Sets the server startup status</li>
   *
   * @throws ServerException thrown if the server could not be initialized.
   */
  public void init() throws ServerException {
    if (status != Status.UNDEF) {
      throw new IllegalStateException("Server already initialized");
    }
    status = Status.BOOTING;
    verifyDir(homeDir);
    verifyDir(tempDir);
    Properties serverInfo = new Properties();
    try {
      InputStream is = getResource(name + ".properties");
      serverInfo.load(is);
      is.close();
    } catch (IOException ex) {
      throw new RuntimeException("Could not load server information file: " + name + ".properties");
    }
    initLog();
    log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
    log.info("Server [{}] starting", name);
    log.info("  Built information:");
    log.info("    Version           : {}", serverInfo.getProperty(name + ".version", "undef"));
    log.info("    Source Repository : {}", serverInfo.getProperty(name + ".source.repository", "undef"));
    log.info("    Source Revision   : {}", serverInfo.getProperty(name + ".source.revision", "undef"));
    log.info("    Built by          : {}", serverInfo.getProperty(name + ".build.username", "undef"));
    log.info("    Built timestamp   : {}", serverInfo.getProperty(name + ".build.timestamp", "undef"));
    log.info("  Runtime information:");
    log.info("    Home   dir: {}", homeDir);
    log.info("    Config dir: {}", (config == null) ? configDir : "-");
    log.info("    Log    dir: {}", logDir);
    log.info("    Temp   dir: {}", tempDir);
    initConfig();
    log.debug("Loading services");
    List<Service> list = loadServices();
    try {
      log.debug("Initializing services");
      initServices(list);
      log.info("Services initialized");
    } catch (ServerException ex) {
      log.error("Services initialization failure, destroying initialized services");
      destroyServices();
      throw ex;
    }
    Status status = Status.valueOf(getConfig().get(getPrefixedName(CONF_STARTUP_STATUS), Status.NORMAL.toString()));
    setStatus(status);
    log.info("Server [{}] started!, status [{}]", name, status);
  }

  /**
   * Verifies the specified directory exists.
   *
   * @param dir directory to verify it exists.
   *
   * @throws ServerException thrown if the directory does not exist or it the
   * path it is not a directory.
   */
  private void verifyDir(String dir) throws ServerException {
    File file = new File(dir);
    if (!file.exists()) {
      throw new ServerException(ServerException.ERROR.S01, dir);
    }
    if (!file.isDirectory()) {
      throw new ServerException(ServerException.ERROR.S02, dir);
    }
  }

  /**
   * Initializes Log4j logging.
   *
   * @throws ServerException thrown if Log4j could not be initialized.
   */
  protected void initLog() throws ServerException {
    verifyDir(logDir);
    LogManager.resetConfiguration();
    File log4jFile = new File(configDir, name + "-log4j.properties");
    if (log4jFile.exists()) {
      PropertyConfigurator.configureAndWatch(log4jFile.toString(), 10 * 1000); //every 10 secs
      log = LoggerFactory.getLogger(Server.class);
    } else {
      Properties props = new Properties();
      try {
        InputStream is = getResource(DEFAULT_LOG4J_PROPERTIES);
        try {
          props.load(is);
        } finally {
          is.close();
        }
      } catch (IOException ex) {
        throw new ServerException(ServerException.ERROR.S03, DEFAULT_LOG4J_PROPERTIES, ex.getMessage(), ex);
      }
      PropertyConfigurator.configure(props);
      log = LoggerFactory.getLogger(Server.class);
      log.warn("Log4j [{}] configuration file not found, using default configuration from classpath", log4jFile);
    }
  }

  /**
   * Loads and inializes the server configuration.
   *
   * @throws ServerException thrown if the configuration could not be loaded/initialized.
   */
  protected void initConfig() throws ServerException {
    verifyDir(configDir);
    File file = new File(configDir);
    Configuration defaultConf;
    String defaultConfig = name + "-default.xml";
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(defaultConfig);
    if (inputStream == null) {
      log.warn("Default configuration file not available in classpath [{}]", defaultConfig);
      defaultConf = new Configuration(false);
    } else {
      try {
        defaultConf = new Configuration(false);
        ConfigurationUtils.load(defaultConf, inputStream);
      } catch (Exception ex) {
        throw new ServerException(ServerException.ERROR.S03, defaultConfig, ex.getMessage(), ex);
      }
    }

    if (config == null) {
      Configuration siteConf;
      File siteFile = new File(file, name + "-site.xml");
      if (!siteFile.exists()) {
        log.warn("Site configuration file [{}] not found in config directory", siteFile);
        siteConf = new Configuration(false);
      } else {
        if (!siteFile.isFile()) {
          throw new ServerException(ServerException.ERROR.S05, siteFile.getAbsolutePath());
        }
        try {
          log.debug("Loading site configuration from [{}]", siteFile);
          inputStream = new FileInputStream(siteFile);
          siteConf = new Configuration(false);
          ConfigurationUtils.load(siteConf, inputStream);
        } catch (IOException ex) {
          throw new ServerException(ServerException.ERROR.S06, siteFile, ex.getMessage(), ex);
        }
      }

      config = new Configuration(false);
      ConfigurationUtils.copy(siteConf, config);
    }

    ConfigurationUtils.injectDefaults(defaultConf, config);

    for (String name : System.getProperties().stringPropertyNames()) {
      String value = System.getProperty(name);
      if (name.startsWith(getPrefix() + ".")) {
        config.set(name, value);
        if (name.endsWith(".password") || name.endsWith(".secret")) {
          value = "*MASKED*";
        }
        log.info("System property sets  {}: {}", name, value);
      }
    }

    log.debug("Loaded Configuration:");
    log.debug("------------------------------------------------------");
    for (Map.Entry<String, String> entry : config) {
      String name = entry.getKey();
      String value = config.get(entry.getKey());
      if (name.endsWith(".password") || name.endsWith(".secret")) {
        value = "*MASKED*";
      }
      log.debug("  {}: {}", entry.getKey(), value);
    }
    log.debug("------------------------------------------------------");
  }

  /**
   * Loads the specified services.
   *
   * @param classes services classes to load.
   * @param list list of loaded service in order of appearance in the
   * configuration.
   *
   * @throws ServerException thrown if a service class could not be loaded.
   */
  private void loadServices(Class[] classes, List<Service> list) throws ServerException {
    for (Class klass : classes) {
      try {
        Service service = (Service) klass.newInstance();
        log.debug("Loading service [{}] implementation [{}]", service.getInterface(),
                  service.getClass());
        if (!service.getInterface().isInstance(service)) {
          throw new ServerException(ServerException.ERROR.S04, klass, service.getInterface().getName());
        }
        list.add(service);
      } catch (ServerException ex) {
        throw ex;
      } catch (Exception ex) {
        throw new ServerException(ServerException.ERROR.S07, klass, ex.getMessage(), ex);
      }
    }
  }

  /**
   * Loads services defined in <code>services</code> and
   * <code>services.ext</code> and de-dups them.
   *
   * @return List of final services to initialize.
   *
   * @throws ServerException throw if the services could not be loaded.
   */
  protected List<Service> loadServices() throws ServerException {
    try {
      Map<Class, Service> map = new LinkedHashMap<Class, Service>();
      Class[] classes = getConfig().getClasses(getPrefixedName(CONF_SERVICES));
      Class[] classesExt = getConfig().getClasses(getPrefixedName(CONF_SERVICES_EXT));
      List<Service> list = new ArrayList<Service>();
      loadServices(classes, list);
      loadServices(classesExt, list);

      //removing duplicate services, strategy: last one wins
      for (Service service : list) {
        if (map.containsKey(service.getInterface())) {
          log.debug("Replacing service [{}] implementation [{}]", service.getInterface(),
                    service.getClass());
        }
        map.put(service.getInterface(), service);
      }
      list = new ArrayList<Service>();
      for (Map.Entry<Class, Service> entry : map.entrySet()) {
        list.add(entry.getValue());
      }
      return list;
    } catch (RuntimeException ex) {
      throw new ServerException(ServerException.ERROR.S08, ex.getMessage(), ex);
    }
  }

  /**
   * Initializes the list of services.
   *
   * @param services services to initialized, it must be a de-dupped list of
   * services.
   *
   * @throws ServerException thrown if the services could not be initialized.
   */
  protected void initServices(List<Service> services) throws ServerException {
    for (Service service : services) {
      log.debug("Initializing service [{}]", service.getInterface());
      checkServiceDependencies(service);
      service.init(this);
      this.services.put(service.getInterface(), service);
    }
    for (Service service : services) {
      service.postInit();
    }
  }

  /**
   * Checks if all service dependencies of a service are available.
   *
   * @param service service to check if all its dependencies are available.
   *
   * @throws ServerException thrown if a service dependency is missing.
   */
  protected void checkServiceDependencies(Service service) throws ServerException {
    if (service.getServiceDependencies() != null) {
      for (Class dependency : service.getServiceDependencies()) {
        if (services.get(dependency) == null) {
          throw new ServerException(ServerException.ERROR.S10, service.getClass(), dependency);
        }
      }
    }
  }

  /**
   * Destroys the server services.
   */
  protected void destroyServices() {
    List<Service> list = new ArrayList<Service>(services.values());
    Collections.reverse(list);
    for (Service service : list) {
      try {
        log.debug("Destroying service [{}]", service.getInterface());
        service.destroy();
      } catch (Throwable ex) {
        log.error("Could not destroy service [{}], {}",
                  new Object[]{service.getInterface(), ex.getMessage(), ex});
      }
    }
    log.info("Services destroyed");
  }

  /**
   * Destroys the server.
   * <p/>
   * All services are destroyed in reverse order of initialization, then the
   * Log4j framework is shutdown.
   */
  public void destroy() {
    ensureOperational();
    destroyServices();
    log.info("Server [{}] shutdown!", name);
    log.info("======================================================");
    if (!Boolean.getBoolean("test.circus")) {
      LogManager.shutdown();
    }
    status = Status.SHUTDOWN;
  }

  /**
   * Returns the name of the server.
   *
   * @return the server name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the server prefix for server configuration properties.
   * <p/>
   * By default it is the server name.
   *
   * @return the prefix for server configuration properties.
   */
  public String getPrefix() {
    return getName();
  }

  /**
   * Returns the prefixed name of a server property.
   *
   * @param name of the property.
   *
   * @return prefixed name of the property.
   */
  public String getPrefixedName(String name) {
    return getPrefix() + "." + Check.notEmpty(name, "name");
  }

  /**
   * Returns the server home dir.
   *
   * @return the server home dir.
   */
  public String getHomeDir() {
    return homeDir;
  }

  /**
   * Returns the server config dir.
   *
   * @return the server config dir.
   */
  public String getConfigDir() {
    return configDir;
  }

  /**
   * Returns the server log dir.
   *
   * @return the server log dir.
   */
  public String getLogDir() {
    return logDir;
  }

  /**
   * Returns the server temp dir.
   *
   * @return the server temp dir.
   */
  public String getTempDir() {
    return tempDir;
  }

  /**
   * Returns the server configuration.
   *
   * @return the server configuration.
   */
  public Configuration getConfig() {
    return config;

  }

  /**
   * Returns the {@link Service} associated to the specified interface.
   *
   * @param serviceKlass service interface.
   *
   * @return the service implementation.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(Class<T> serviceKlass) {
    ensureOperational();
    Check.notNull(serviceKlass, "serviceKlass");
    return (T) services.get(serviceKlass);
  }

  /**
   * Adds a service programmatically.
   * <p/>
   * If a service with the same interface exists, it will be destroyed and
   * removed before the given one is initialized and added.
   * <p/>
   * If an exception is thrown the server is destroyed.
   *
   * @param klass service class to add.
   *
   * @throws ServerException throw if the service could not initialized/added
   * to the server.
   */
  public void setService(Class<? extends Service> klass) throws ServerException {
    ensureOperational();
    Check.notNull(klass, "serviceKlass");
    if (getStatus() == Status.SHUTTING_DOWN) {
      throw new IllegalStateException("Server shutting down");
    }
    try {
      Service newService = klass.newInstance();
      Service oldService = services.get(newService.getInterface());
      if (oldService != null) {
        try {
          oldService.destroy();
        } catch (Throwable ex) {
          log.error("Could not destroy service [{}], {}",
                    new Object[]{oldService.getInterface(), ex.getMessage(), ex});
        }
      }
      newService.init(this);
      services.put(newService.getInterface(), newService);
    } catch (Exception ex) {
      log.error("Could not set service [{}] programmatically -server shutting down-, {}", klass, ex);
      destroy();
      throw new ServerException(ServerException.ERROR.S09, klass, ex.getMessage(), ex);
    }
  }

}
