package org.apache.hadoop.util.concurrent;

import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.apache.hadoop.util.SubjectUtil;

/**
 * Helper class to restore Subject propagation behavior after the JEP411/JEP486 changes
 * 
 * This is shim for the cases where the run() method is directly overridden in the Thread class.
 */
public class HadoopThread extends Thread {
  
  Subject startSubject;
  Runnable hadoopTarget;
  
  public HadoopThread() {
    super();
  }
  
  public HadoopThread(Runnable target) {
    super();
    this.hadoopTarget = target;
  }
  
  public HadoopThread(ThreadGroup group, Runnable target) {
    // The target passed to Thread has no effect, we only pass it
    // because there is no super(group) constructor.
    super(group, target);
    this.hadoopTarget = target;
  }
  
  public HadoopThread(Runnable target, String name) {
    super(name);
    this.hadoopTarget = target;
  }
  
  public HadoopThread(String name) {
    super(name);
  }
  
  public HadoopThread(ThreadGroup group, String name) {
    super(group, name);
  }
  
  public HadoopThread(ThreadGroup group, Runnable target, String name) {
    super(group, name);
    this.hadoopTarget = target;
  }
  
  @Override
  public final void start() {
    startSubject = SubjectUtil.current();
    super.start();
  }
  
  /**
   * Override this instead of run()
   */
  public void work() {
    throw new IllegalArgumentException("No Runnable was specified and work() is not overriden");
  }
  
  @Override
  public final void run() {
    SubjectUtil.callAs(startSubject, new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        if (hadoopTarget != null) {
          hadoopTarget.run();
        } else {
          work();
        }
        return null;
      }
      
    });
  }
 }
