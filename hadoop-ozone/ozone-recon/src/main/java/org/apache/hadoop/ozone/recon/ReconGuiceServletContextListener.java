package org.apache.hadoop.ozone.recon;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;

/**
 * Servlet Context Listener that provides the Guice injector.
 */
public class ReconGuiceServletContextListener
    extends GuiceServletContextListener {

  private static Injector injector;

  @Override
  public Injector getInjector() {
    return injector;
  }

  static void setInjector(Injector inj) {
    injector = inj;
  }
}
