/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.raid;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Galois field arithmetics with 2^p elements.
 * The input must be unsigned integers.
 */
public class GaloisField {
  
  private final int[] logTable;
  private final int[] powTable;
  private final int[][] mulTable;
  private final int[][] divTable;
  private final int fieldSize;
  private final int primitivePeriod;
  private final int primitivePolynomial;

  // Field size 256 is good for byte based system
  private static final int DEFAULT_FIELD_SIZE = 256;
  // primitive polynomial 1 + X^2 + X^3 + X^4 + X^8
  private static final int DEFAULT_PRIMITIVE_POLYNOMIAL = 285;

  static private final Map<Integer, GaloisField> instances =
    new HashMap<Integer, GaloisField>();

  /**
   * Get the object performs Galois field arithmetics
   * @param fieldSize size of the field
   * @param primitivePolynomial a primitive polynomial corresponds to the size
   */
  public static GaloisField getInstance(int fieldSize,
      int primitivePolynomial) {
    int key = ((fieldSize << 16) & 0xFFFF0000) + (primitivePolynomial & 0x0000FFFF);
    GaloisField gf;
    synchronized (instances) {
      gf = instances.get(key);
      if (gf == null) {
        gf = new GaloisField(fieldSize, primitivePolynomial);
        instances.put(key, gf);
      }
    }
    return gf;
  }

  /**
   * Get the object performs Galois field arithmetics with default setting
   */
  public static GaloisField getInstance() {
    return getInstance(DEFAULT_FIELD_SIZE, DEFAULT_PRIMITIVE_POLYNOMIAL);
  }

  private GaloisField(int fieldSize, int primitivePolynomial) {
    assert fieldSize > 0;
    assert primitivePolynomial > 0;

    this.fieldSize = fieldSize;
    this.primitivePeriod = fieldSize - 1;
    this.primitivePolynomial = primitivePolynomial;
    logTable = new int[fieldSize];
    powTable = new int[fieldSize];
    mulTable = new int[fieldSize][fieldSize];
    divTable = new int[fieldSize][fieldSize];
    int value = 1;
    for (int pow = 0; pow < fieldSize - 1; pow++) {
      powTable[pow] = value;
      logTable[value] = pow;
      value = value * 2;
      if (value >= fieldSize) {
        value = value ^ primitivePolynomial;
      }
    }
    // building multiplication table
    for (int i = 0; i < fieldSize; i++) {
      for (int j = 0; j < fieldSize; j++) {
        if (i == 0 || j == 0) {
          mulTable[i][j] = 0;
          continue;
        }
        int z = logTable[i] + logTable[j];
        z = z >= primitivePeriod ? z - primitivePeriod : z;
        z = powTable[z];
        mulTable[i][j] = z;
      }
    }
    // building division table
    for (int i = 0; i < fieldSize; i++) {
      for (int j = 1; j < fieldSize; j++) {
        if (i == 0) {
          divTable[i][j] = 0;
          continue;
        }
        int z = logTable[i] - logTable[j];
        z = z < 0 ? z + primitivePeriod : z;
        z = powTable[z];
        divTable[i][j] = z;
      }
    }
  }

  /**
   * Return number of elements in the field
   * @return number of elements in the field
   */
  public int getFieldSize() {
    return fieldSize;
  }

  /**
   * Return the primitive polynomial in GF(2)
   * @return primitive polynomial as a integer
   */
  public int getPrimitivePolynomial() {
    return primitivePolynomial;
  }

  /**
   * Compute the sum of two fields
   * @param x input field
   * @param y input field
   * @return result of addition
   */
  public int add(int x, int y) {
    assert(x >= 0 && x < getFieldSize() && y >= 0 && y < getFieldSize());
    return x ^ y;
  }

  /**
   * Compute the multiplication of two fields
   * @param x input field
   * @param y input field
   * @return result of multiplication
   */
  public int multiply(int x, int y) {
    assert(x >= 0 && x < getFieldSize() && y >= 0 && y < getFieldSize());
    return mulTable[x][y];
  }

  /**
   * Compute the division of two fields
   * @param x input field
   * @param y input field
   * @return x/y
   */
  public int divide(int x, int y) {
    assert(x >= 0 && x < getFieldSize() && y > 0 && y < getFieldSize());
    return divTable[x][y];
  }

  /**
   * Compute power n of a field
   * @param x input field
   * @param n power
   * @return x^n
   */
  public int power(int x, int n) {
    assert(x >= 0 && x < getFieldSize());
    if (n == 0) {
      return 1;
    }
    if (x == 0) {
      return 0;
    }
    x = logTable[x] * n;
    if (x < primitivePeriod) {
      return powTable[x];
    }
    x = x % primitivePeriod;
    return powTable[x];
  }

  /**
   * Given a Vandermonde matrix V[i][j]=x[j]^i and vector y, solve for z such
   * that Vz=y. The output z will be placed in y.
   * @param x the vector which describe the Vandermonde matrix
   * @param y right-hand side of the Vandermonde system equation.
   *          will be replaced the output in this vector
   */
  public void solveVandermondeSystem(int[] x, int[] y) {
    solveVandermondeSystem(x, y, x.length);
  }

  /**
   * Given a Vandermonde matrix V[i][j]=x[j]^i and vector y, solve for z such
   * that Vz=y. The output z will be placed in y.
   * @param x the vector which describe the Vandermonde matrix
   * @param y right-hand side of the Vandermonde system equation.
   *          will be replaced the output in this vector
   * @param len consider x and y only from 0...len-1
   */
  public void solveVandermondeSystem(int[] x, int[] y, int len) {
    assert(x.length <= len && y.length <= len);
    for (int i = 0; i < len - 1; i++) {
      for (int j = len - 1; j > i; j--) {
        y[j] = y[j] ^ mulTable[x[i]][y[j - 1]];
      }
    }
    for (int i = len - 1; i >= 0; i--) {
      for (int j = i + 1; j < len; j++) {
        y[j] = divTable[y[j]][x[j] ^ x[j - i - 1]];
      }
      for (int j = i; j < len - 1; j++) {
        y[j] = y[j] ^ y[j + 1];
      }
    }
  }

  /**
   * Compute the multiplication of two polynomials. The index in the
   * array corresponds to the power of the entry. For example p[0] is the
   * constant term of the polynomial p. 
   * @param p input polynomial
   * @param q input polynomial
   * @return polynomial represents p*q
   */
  public int[] multiply(int[] p, int[] q) {
    int len = p.length + q.length - 1;
    int[] result = new int[len];
    for (int i = 0; i < len; i++) {
      result[i] = 0;
    }
    for (int i = 0; i < p.length; i++) {
      for (int j = 0; j < q.length; j++) {
        result[i + j] = add(result[i + j], multiply(p[i], q[j]));
      }
    }
    return result;
  }

  /**
   * Compute the remainder of a dividend and divisor pair. The index in the
   * array corresponds to the power of the entry. For example p[0] is the
   * constant term of the polynomial p. 
   * @param dividend dividend polynomial, the remainder will be placed here when return
   * @param divisor divisor polynomial
   */
  public void remainder(int[] dividend, int[] divisor) {
    for (int i = dividend.length - divisor.length; i >= 0; i--) {
      int ratio =
        divTable[dividend[i + divisor.length - 1]][divisor[divisor.length - 1]];
      for (int j = 0; j < divisor.length; j++) {
        int k = j + i;
        dividend[k] = dividend[k] ^ mulTable[ratio][divisor[j]];
      }
    }
  }

  /**
   * Compute the sum of two polynomials. The index in the
   * array corresponds to the power of the entry. For example p[0] is the
   * constant term of the polynomial p. 
   * @param p input polynomial
   * @param q input polynomial
   * @return polynomial represents p+q
   */
  public int[] add(int[] p, int[] q) {
    int len = Math.max(p.length, q.length);
    int[] result = new int[len];
    for (int i = 0; i < len; i++) {
      if (i < p.length && i < q.length) {
        result[i] = add(p[i], q[i]);
      } else if (i < p.length) {
        result[i] = p[i];
      } else {
        result[i] = q[i];
      }
    }
    return result;
  }

  /**
   * Substitute x into polynomial p(x). 
   * @param p input polynomial
   * @param x input field
   * @return p(x)
   */
  public int substitute(int[] p, int x) {
    int result = 0;
    int y = 1;
    for (int i = 0; i < p.length; i++) {
      result = result ^ mulTable[p[i]][y];
      y = mulTable[x][y];
    }
    return result;
  }
}
