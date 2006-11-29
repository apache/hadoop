package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A wrapper for Writable instances.
 * <p>
 * When two sequence files, which have same Key type but different Value
 * types, are mapped out to reduce, multiple Value types is not allowed.
 * In this case, this class can help you wrap instances with different types.
 * </p>
 * 
 * <p>
 * Compared with <code>ObjectWritable</code>, this class is much more effective,
 * because <code>ObjectWritable</code> will append the class declaration as a String 
 * into the output file in every Key-Value pair.
 * </p>
 * 
 * how to use it: <br>
 * 1. Write your own class, such as GenericObject, which extends GenericWritable.<br> 
 * 2. Implements the abstract method <code>getTypes()</code>, defines 
 *    the classes which will be wrapped in GenericObject in application.
 *    Attention: this classes defined in <code>getTypes()</code> method, must
 *    implement <code>Writable</code> interface.
 * <br><br>
 * 
 * The code looks like this:
 * <blockquote><pre>
 * public class GenericObject extends GenericWritable {
 * 
 *   private static Class[] CLASSES = {
 *               ClassType1.class, 
 *               ClassType2.class,
 *               ClassType3.class,
 *               };
 *
 *   protected Class[] getTypes() {
 *       return CLASSES;
 *   }
 *
 * }
 * </pre></blockquote>
 * 
 * @author Feng Jiang (Feng.a.Jiang@gmail.com)
 * @since Nov 8, 2006
 */
public abstract class GenericWritable implements Writable {

  private static final byte NOT_SET = -1;

  private byte type = NOT_SET;

  private Writable instance;

  /**
   * Set the instance that is wrapped.
   * 
   * @param obj
   */
  public void set(Writable obj) {
    instance = obj;
    Class[] clazzes = getTypes();
    for (int i = 0; i < clazzes.length; i++) {
      Class clazz = clazzes[i];
      if (clazz.isInstance(instance)) {
        type = (byte) i;
        return;
      }
    }
    throw new RuntimeException("The type of instance is: "
                + instance.getClass() + ", which is NOT registered.");
  }

  /**
   * Return the wrapped instance.
   */
  public Writable get() {
    return instance;
  }

  public void readFields(DataInput in) throws IOException {
    type = in.readByte();
    Class clazz = getTypes()[type];
    try {
      instance = (Writable) clazz.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException("Cannot initialize the class: " + clazz);
    }
    instance.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    if (type == NOT_SET || instance == null)
      throw new IOException("The GenericWritable has NOT been set correctly. type="
                            + type + ", instance=" + instance);
    out.writeByte(type);
    instance.write(out);
  }

  /**
   * Return all classes that may be wrapped.  Subclasses should implement this
   * to return a constant array of classes.
   */
  abstract protected Class[] getTypes();

}
