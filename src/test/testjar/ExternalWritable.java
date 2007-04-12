package testjar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This is an example simple writable class.  This is used as a class external 
 * to the Hadoop IO classes for testing of user Writable classes.
 * 
 * @author Dennis E. Kubes
 */
public class ExternalWritable
  implements WritableComparable {

  private String message = null;
  
  public ExternalWritable() {
    
  }
  
  public ExternalWritable(String message) {
    this.message = message;
  }
  
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public void readFields(DataInput in)
    throws IOException {
    
    message = null;
    boolean hasMessage = in.readBoolean();
    if (hasMessage) {
      message = in.readUTF();   
    }
  }

  public void write(DataOutput out)
    throws IOException {
    
    boolean hasMessage = (message != null && message.length() > 0);
    out.writeBoolean(hasMessage);
    if (hasMessage) {
      out.writeUTF(message);
    }
  }
  
  public int compareTo(Object o) {
    
    if (!(o instanceof ExternalWritable)) {
      throw new IllegalArgumentException("Input not an ExternalWritable");
    }
    
    ExternalWritable that = (ExternalWritable)o;
    return this.message.compareTo(that.message);
  }

  public String toString() {
    return this.message;
  }
}
