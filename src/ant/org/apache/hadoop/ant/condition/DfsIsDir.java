package org.apache.hadoop.ant.condition;

public class DfsIsDir extends DfsBaseConditional {
  protected final char flag = 'd';
  protected char getFlag() { return flag; }
}
