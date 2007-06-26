package org.apache.hadoop.ant.condition;

public class DfsZeroLen extends DfsBaseConditional {
  protected final char flag = 'z';
  protected char getFlag() { return flag; }
}
