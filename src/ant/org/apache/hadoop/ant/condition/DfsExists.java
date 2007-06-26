package org.apache.hadoop.ant.condition;

public class DfsExists extends DfsBaseConditional {
  protected final char flag = 'e';
  protected char getFlag() { return flag; }
}
