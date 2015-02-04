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
package org.apache.hadoop.examples.pi.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.NoSuchElementException;

import org.apache.hadoop.examples.pi.Container;
import org.apache.hadoop.examples.pi.Util;

/**
 * Bellard's BBP-type Pi formula
 * 1/2^6 \sum_{n=0}^\infty (-1)^n/2^{10n}
 * (-2^5/(4n+1) -1/(4n+3) +2^8/(10n+1) -2^6/(10n+3) -2^2/(10n+5)
 *  -2^2/(10n+7) +1/(10n+9))
 *  
 * References:
 *
 * [1] David H. Bailey, Peter B. Borwein and Simon Plouffe.  On the Rapid
 *     Computation of Various Polylogarithmic Constants.
 *     Math. Comp., 66:903-913, 1996.
 *     
 * [2] Fabrice Bellard.  A new formula to compute the n'th binary digit of pi,
 *     1997.  Available at http://fabrice.bellard.free.fr/pi .
 */
public final class Bellard {
  /** Parameters for the sums */
  public enum Parameter {
    // \sum_{k=0}^\infty (-1)^{k+1}( 2^{d-10k-1}/(4k+1) + 2^{d-10k-6}/(4k+3) )
    P8_1(false, 1, 8, -1),
    P8_3(false, 3, 8, -6),
    P8_5(P8_1),
    P8_7(P8_3),

    /*
     *   2^d\sum_{k=0}^\infty (-1)^k( 2^{ 2-10k} / (10k + 1)
     *                               -2^{  -10k} / (10k + 3)
     *                               -2^{-4-10k} / (10k + 5)
     *                               -2^{-4-10k} / (10k + 7)
     *                               +2^{-6-10k} / (10k + 9) )
     */
    P20_21(true , 1, 20,  2),
    P20_3(false, 3, 20,  0),
    P20_5(false, 5, 20, -4),
    P20_7(false, 7, 20, -4),
    P20_9(true , 9, 20, -6),
    P20_11(P20_21),
    P20_13(P20_3),
    P20_15(P20_5),
    P20_17(P20_7),
    P20_19(P20_9);
    
    final boolean isplus;
    final long j;
    final int deltaN;
    final int deltaE;
    final int offsetE;      

    private Parameter(boolean isplus, long j, int deltaN, int offsetE) {
      this.isplus = isplus;
      this.j = j;
      this.deltaN = deltaN;
      this.deltaE = -20;
      this.offsetE = offsetE;        
    }

    private Parameter(Parameter p) {
      this.isplus = !p.isplus;
      this.j = p.j + (p.deltaN >> 1);
      this.deltaN = p.deltaN;
      this.deltaE = p.deltaE;
      this.offsetE = p.offsetE + (p.deltaE >> 1);
    }

    /** Get the Parameter represented by the String */
    public static Parameter get(String s) {
      s = s.trim();
      if (s.charAt(0) == 'P')
        s = s.substring(1);
      final String[] parts = s.split("\\D+");
      if (parts.length >= 2) {
        final String name = "P" + parts[0] + "_" + parts[1];  
        for(Parameter p : values())
          if (p.name().equals(name))
            return p;
      }
      throw new IllegalArgumentException("s=" + s
          + ", parts=" + Arrays.asList(parts));
    }
  }

  /** The sums in the Bellard's formula */
  public static class Sum implements Container<Summation>, Iterable<Summation> {
    private static final long ACCURACY_BIT = 50;

    private final Parameter parameter;
    private final Summation sigma;
    private final Summation[] parts;
    private final Tail tail;

    /** Constructor */
    private <T extends Container<Summation>> Sum(long b, Parameter p, int nParts, List<T> existing) {
      if (b < 0)
        throw new IllegalArgumentException("b = " + b + " < 0");
      if (nParts < 1)
        throw new IllegalArgumentException("nParts = " + nParts + " < 1");
      final long i = p.j == 1 && p.offsetE >= 0? 1 : 0;
      final long e = b + i*p.deltaE + p.offsetE;
      final long n = i*p.deltaN + p.j;

      this.parameter = p;
      this.sigma = new Summation(n, p.deltaN, e, p.deltaE, 0);
      this.parts = partition(sigma, nParts, existing);
      this.tail = new Tail(n, e);
    }

    private static <T extends Container<Summation>> Summation[] partition(
        Summation sigma, int nParts, List<T> existing) {
      final List<Summation> parts = new ArrayList<Summation>();
      if (existing == null || existing.isEmpty())
        parts.addAll(Arrays.asList(sigma.partition(nParts)));
      else {
        final long stepsPerPart = sigma.getSteps()/nParts;
        final List<Summation> remaining = sigma.remainingTerms(existing);

        for(Summation s : remaining) {
          final int n = (int)((s.getSteps() - 1)/stepsPerPart) + 1;
          parts.addAll(Arrays.asList(s.partition(n)));
        }
        
        for(Container<Summation> c : existing)
          parts.add(c.getElement());
        Collections.sort(parts);
      }
      return parts.toArray(new Summation[parts.size()]);
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
      int n = 0;
      for(Summation s : parts)
        if (s.getValue() == null)
          n++;
      return getClass().getSimpleName() + "{" + parameter + ": " + sigma
          + ", remaining=" + n + "}";
    }

    /** Set the value of sigma */
    public void setValue(Summation s) {
      if (s.getValue() == null)
        throw new IllegalArgumentException("s.getValue()"
            + "\n  sigma=" + sigma
            + "\n  s    =" + s);
      if (!s.contains(sigma) || !sigma.contains(s))
        throw new IllegalArgumentException("!s.contains(sigma) || !sigma.contains(s)"
            + "\n  sigma=" + sigma
            + "\n  s    =" + s);
      sigma.setValue(s.getValue());      
    }

    /** get the value of sigma */
    public double getValue() {
      if (sigma.getValue() == null) {
        double d = 0;
        for(int i = 0; i < parts.length; i++)
          d = Modular.addMod(d, parts[i].compute());
        sigma.setValue(d);
      }

      final double s = Modular.addMod(sigma.getValue(), tail.compute()); 
      return parameter.isplus? s: -s;
    }
    
    /** {@inheritDoc} */
    @Override
    public Summation getElement() {
      if (sigma.getValue() == null) {
        int i = 0;
        double d = 0;
        for(; i < parts.length && parts[i].getValue() != null; i++)
          d = Modular.addMod(d, parts[i].getValue());
        if (i == parts.length)
          sigma.setValue(d);
      }
      return sigma;
    }

    /** The sum tail */
    private class Tail {
      private long n;
      private long e;
      
      private Tail(long n, long e) {
        this.n = n;
        this.e = e;
      }

      private double compute() {
        if (e > 0) {
          final long edelta = -sigma.E.delta;
          long q = e / edelta;
          long r = e % edelta;
          if (r == 0) {
            e = 0;
            n += q * sigma.N.delta;
          } else {
            e = edelta - r;
            n += (q + 1)*sigma.N.delta;
          }
        } else if (e < 0)
          e = -e; 
    
        double s = 0;
        for(;; e -= sigma.E.delta) {
          if (e > ACCURACY_BIT || (1L << (ACCURACY_BIT - e)) < n)
            return s;
    
          s += 1.0 / (n << e);
          if (s >= 1) s--;
          n += sigma.N.delta;
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Summation> iterator() {
      return new Iterator<Summation>() {
        private int i = 0;

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {return i < parts.length;}
        /** {@inheritDoc} */
        @Override
        public Summation next() throws NoSuchElementException {
          if (hasNext()) {
            return parts[i++];
          } else {
            throw new NoSuchElementException("Sum's iterator does not have next!");
          }
        }
        /** Unsupported */
        @Override
        public void remove() {throw new UnsupportedOperationException();}
      };
    }
  }

  /** Get the sums for the Bellard formula. */
  public static <T extends Container<Summation>> Map<Parameter, Sum> getSums(
      long b, int partsPerSum, Map<Parameter, List<T>> existing) {
    final Map<Parameter, Sum> sums = new TreeMap<Parameter, Sum>();
    for(Parameter p : Parameter.values()) {
      final Sum s = new Sum(b, p, partsPerSum, existing.get(p));
      Util.out.println("put " + s);
      sums.put(p, s);
    }
    return sums;
  }

  /** Compute bits of Pi from the results. */
  public static <T extends Container<Summation>> double computePi(
      final long b, Map<Parameter, T> results) {
    if (results.size() != Parameter.values().length)
      throw new IllegalArgumentException("m.size() != Parameter.values().length"
          + ", m.size()=" + results.size()
          + "\n  m=" + results);

    double pi = 0;
    for(Parameter p : Parameter.values()) {
      final Summation sigma = results.get(p).getElement();
      final Sum s = new Sum(b, p, 1, null);
      s.setValue(sigma);
      pi = Modular.addMod(pi, s.getValue());
    }
    return pi;
  }

  /** Compute bits of Pi in the local machine. */
  public static double computePi(final long b) {
    double pi = 0;
    for(Parameter p : Parameter.values())
      pi = Modular.addMod(pi, new Sum(b, p, 1, null).getValue());
    return pi;
  }

  /** Estimate the number of terms. */
  public static long bit2terms(long b) {
    return 7*(b/10);
  }

  private static void computePi(Util.Timer t, long b) {
    t.tick(Util.pi2string(computePi(b), bit2terms(b)));
  }

  /** main */
  public static void main(String[] args) throws IOException {
    final Util.Timer t = new Util.Timer(false);

    computePi(t, 0);
    computePi(t, 1);
    computePi(t, 2);
    computePi(t, 3);
    computePi(t, 4);

    Util.printBitSkipped(1008);
    computePi(t, 1008);
    computePi(t, 1012);

    long b = 10;
    for(int i = 0; i < 7; i++) {
      Util.printBitSkipped(b);
      computePi(t, b - 4);
      computePi(t, b);
      computePi(t, b + 4);
      b *= 10;
    }
  }
}
