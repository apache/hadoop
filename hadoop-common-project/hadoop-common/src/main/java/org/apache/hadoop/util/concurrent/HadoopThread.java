package org.apache.hadoop.util.concurrent;

import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.apache.hadoop.util.SubjectUtil;

/**
 * Helper class to restore Subject propagation behavior after the JEP411/JEP486 changes
 * 
 * This is shim for the cases where the run() method is directly overridden in the Thread class.
 */
public abstract class HadoopThread extends Thread {
  
  Subject startSubject;
  
  public HadoopThread() {
    super();
  }
  
  public HadoopThread(String name) {
    super(name);
  }
  
  public HadoopThread(ThreadGroup group, String name) {
    super(group, name);
  }
  
  @Override
  public final void start() {
    startSubject = SubjectUtil.current();
    super.start();
  }
  
  /**
   * Override this instead of run()
   */
  public abstract void work();
  
  @Override
  public final void run() {
    SubjectUtil.callAs(startSubject, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        work();
        return null;
      }
      
    });
  }
 }
