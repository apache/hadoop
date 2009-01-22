package org.apache.hadoop.hbase.util;

import java.io.Serializable;

/**
 * A generic class for pairs.
 */
public class Pair<T1, T2> implements Serializable
{
  private static final long serialVersionUID = -3986244606585552569L;
  protected T1 first = null;
  protected T2 second = null;

  /**
   * Default constructor.
   */
  public Pair()
  {
  }

  /**
   * Constructor
   * @param a
   * @param b
   */
  public Pair(T1 a, T2 b)
  {
    this.first = a;
    this.second = b;
  }

  /**
   * Replace the first element of the pair.
   * @param a
   */
  public void setFirst(T1 a)
  {
    this.first = a;
  }

  /**
   * Replace the second element of the pair.
   * @param b 
   */
  public void setSecond(T2 b)
  {
    this.second = b;
  }

  /**
   * Return the first element stored in the pair.
   */
  public T1 getFirst()
  {
    return first;
  }

  /**
   * Return the second element stored in the pair.
   */
  public T2 getSecond()
  {
    return second;
  }

  private static boolean equals(Object x, Object y)
  {
     return (x == null && y == null) || (x != null && x.equals(y));
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other)
  {
    return other instanceof Pair && equals(first, ((Pair)other).first) &&
      equals(second, ((Pair)other).second);
  }

  @Override
  public int hashCode()
  {
    if (first == null)
      return (second == null) ? 0 : second.hashCode() + 1;
    else if (second == null)
      return first.hashCode() + 2;
    else
      return first.hashCode() * 17 + second.hashCode();
  }

  @Override
  public String toString()
  {
    return "{" + getFirst() + "," + getSecond() + "}";
  }
}