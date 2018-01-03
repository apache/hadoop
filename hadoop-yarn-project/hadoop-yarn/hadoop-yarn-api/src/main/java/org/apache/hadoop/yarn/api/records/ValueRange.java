package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.util.Records;

public abstract class ValueRange implements Comparable<ValueRange> {

  public abstract int getBegin();

  public abstract int getEnd();

  public abstract void setBegin(int value);

  public abstract void setEnd(int value);

  public abstract boolean isLessOrEqual(ValueRange other);

  public static ValueRange newInstance(int begin, int end) {
    ValueRange valueRange = Records.newRecord(ValueRange.class);
    valueRange.setBegin(begin);
    valueRange.setEnd(end);
    return valueRange;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (getBegin() == getEnd()) {
      result.append(getBegin());
    } else {
      result.append("[" + getBegin() + "-" + getEnd() + "]");
    }
    return result.toString();
  }

  @Override
  public int compareTo(ValueRange other) {
    if (other == null) {
      return -1;
    }

    if (getBegin() == other.getBegin() && getEnd() == other.getEnd()) {
      return 0;
    } else if (getBegin() - other.getBegin() < 0) {
      return -1;
    } else if (getBegin() - other.getBegin() == 0
        && getEnd() - other.getEnd() < 0) {
      return -1;
    } else {
      return 1;
    }

  }

  @Override
  public ValueRange clone() {
    return ValueRange.newInstance(getBegin(), getEnd());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ValueRange))
      return false;
    ValueRange other = (ValueRange) obj;
    if (getBegin() == other.getBegin() && getEnd() == other.getEnd()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 263167;
    int result = 0;
    result = prime * result + this.getBegin();
    result = prime * result + this.getEnd();
    return result;
  }
}
