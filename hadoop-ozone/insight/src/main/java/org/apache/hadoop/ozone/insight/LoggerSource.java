package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.ozone.insight.Component.Type;

/**
 * Definition of a log source.
 */
public class LoggerSource {

  /**
   * Id of the component where the log is generated.
   */
  private Component component;

  /**
   * Log4j/slf4j logger name.
   */
  private String loggerName;

  /**
   * Log level.
   */
  private Level level;

  public LoggerSource(Component component, String loggerName, Level level) {
    this.component = component;
    this.loggerName = loggerName;
    this.level = level;
  }

  public LoggerSource(Type componentType, Class<?> loggerClass,
      Level level) {
    this(new Component(componentType), loggerClass.getCanonicalName(), level);
  }

  public Component getComponent() {
    return component;
  }

  public String getLoggerName() {
    return loggerName;
  }

  public Level getLevel() {
    return level;
  }

  /**
   * Log level definition.
   */
  public enum Level {
    TRACE, DEBUG, INFO, WARN, ERROR
  }

}
