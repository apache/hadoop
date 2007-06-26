package org.apache.hadoop.ant.condition;

import org.apache.tools.ant.taskdefs.condition.Condition;

/**
 * This wrapper around {@link org.apache.hadoop.ant.DfsTask} implements the
 * Ant &gt;1.5
 * {@link org.apache.tools.ant.taskdefs.condition.Condition Condition}
 * interface for HDFS tests. So one can test conditions like this:
 * {@code
 *   <condition property="precond">
 *     <and>
 *       <hadoop:exists file="fileA" />
 *       <hadoop:exists file="fileB" />
 *       <hadoop:sizezero file="fileB" />
 *     </and>
 *   </condition>
 * }
 * This will define the property precond if fileA exists and fileB has zero
 * length.
 */
public abstract class DfsBaseConditional extends org.apache.hadoop.ant.DfsTask
                       implements Condition {

  protected boolean result;
  String file;

  private void initArgs() {
    setCmd("test");
    setArgs("-"  +  getFlag() + "," + file);
  }

  public void setFile(String file) {
    this.file = file;
  }

  protected abstract char getFlag();

  protected int postCmd(int exit_code) {
    exit_code = super.postCmd(exit_code);
    result = exit_code == 1;
    return exit_code;
  }

  public boolean eval() {
    initArgs();
    execute();
    return result;
  }
}
