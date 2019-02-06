/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.security.ssl;

import java.io.IOException;
import java.io.InputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 ************************************************************************
 * Copied from the not-yet-commons-ssl project at
 * http://juliusdavies.ca/commons-ssl/
 * This project is not yet in Apache, but it is Apache 2.0 licensed.
 ************************************************************************
 * Interface for checking if a hostname matches the names stored inside the
 * server's X.509 certificate.  Correctly implements
 * javax.net.ssl.HostnameVerifier, but that interface is not recommended.
 * Instead we added several check() methods that take SSLSocket,
 * or X509Certificate, or ultimately (they all end up calling this one),
 * String.  (It's easier to supply JUnit with Strings instead of mock
 * SSLSession objects!)
 * <p>Our check() methods throw exceptions if the name is
 * invalid, whereas javax.net.ssl.HostnameVerifier just returns true/false.
 * <p>
 * We provide the HostnameVerifier.DEFAULT, HostnameVerifier.STRICT, and
 * HostnameVerifier.ALLOW_ALL implementations.  We also provide the more
 * specialized HostnameVerifier.DEFAULT_AND_LOCALHOST, as well as
 * HostnameVerifier.STRICT_IE6.  But feel free to define your own
 * implementations!
 * <p>
 * Inspired by Sebastian Hauer's original StrictSSLProtocolSocketFactory in the
 * HttpClient "contrib" repository.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface SSLHostnameVerifier extends javax.net.ssl.HostnameVerifier {

    @Override
    boolean verify(String host, SSLSession session);

    void check(String host, SSLSocket ssl) throws IOException;

    void check(String host, X509Certificate cert) throws SSLException;

    void check(String host, String[] cns, String[] subjectAlts)
        throws SSLException;

    void check(String[] hosts, SSLSocket ssl) throws IOException;

    void check(String[] hosts, X509Certificate cert) throws SSLException;


    /**
     * Checks to see if the supplied hostname matches any of the supplied CNs
     * or "DNS" Subject-Alts.  Most implementations only look at the first CN,
     * and ignore any additional CNs.  Most implementations do look at all of
     * the "DNS" Subject-Alts. The CNs or Subject-Alts may contain wildcards
     * according to RFC 2818.
     *
     * @param cns         CN fields, in order, as extracted from the X.509
     *                    certificate.
     * @param subjectAlts Subject-Alt fields of type 2 ("DNS"), as extracted
     *                    from the X.509 certificate.
     * @param hosts       The array of hostnames to verify.
     * @throws SSLException If verification failed.
     */
    void check(String[] hosts, String[] cns, String[] subjectAlts)
        throws SSLException;


    /**
     * The DEFAULT HostnameVerifier works the same way as Curl and Firefox.
     * <p>
     * The hostname must match either the first CN, or any of the subject-alts.
     * A wildcard can occur in the CN, and in any of the subject-alts.
     * <p>
     * The only difference between DEFAULT and STRICT is that a wildcard (such
     * as "*.foo.com") with DEFAULT matches all subdomains, including
     * "a.b.foo.com".
     */
    public final static SSLHostnameVerifier DEFAULT =
        new AbstractVerifier() {
            @Override
            public final void check(final String[] hosts, final String[] cns,
                                    final String[] subjectAlts)
                throws SSLException {
                check(hosts, cns, subjectAlts, false, false);
            }

            @Override
            public final String toString() { return "DEFAULT"; }
        };


    /**
     * The DEFAULT_AND_LOCALHOST HostnameVerifier works like the DEFAULT
     * one with one additional relaxation:  a host of "localhost",
     * "localhost.localdomain", "127.0.0.1", "::1" will always pass, no matter
     * what is in the server's certificate.
     */
    public final static SSLHostnameVerifier DEFAULT_AND_LOCALHOST =
        new AbstractVerifier() {
            @Override
            public final void check(final String[] hosts, final String[] cns,
                                    final String[] subjectAlts)
                throws SSLException {
                if (isLocalhost(hosts[0])) {
                    return;
                }
                check(hosts, cns, subjectAlts, false, false);
            }

            @Override
            public final String toString() { return "DEFAULT_AND_LOCALHOST"; }
        };

    /**
     * The STRICT HostnameVerifier works the same way as java.net.URL in Sun
     * Java 1.4, Sun Java 5, Sun Java 6.  It's also pretty close to IE6.
     * This implementation appears to be compliant with RFC 2818 for dealing
     * with wildcards.
     * <p>
     * The hostname must match either the first CN, or any of the subject-alts.
     * A wildcard can occur in the CN, and in any of the subject-alts.  The
     * one divergence from IE6 is how we only check the first CN.  IE6 allows
     * a match against any of the CNs present.  We decided to follow in
     * Sun Java 1.4's footsteps and only check the first CN.
     * <p>
     * A wildcard such as "*.foo.com" matches only subdomains in the same
     * level, for example "a.foo.com".  It does not match deeper subdomains
     * such as "a.b.foo.com".
     */
    public final static SSLHostnameVerifier STRICT =
        new AbstractVerifier() {
            @Override
            public final void check(final String[] host, final String[] cns,
                                    final String[] subjectAlts)
                throws SSLException {
                check(host, cns, subjectAlts, false, true);
            }

            @Override
            public final String toString() { return "STRICT"; }
        };

    /**
     * The STRICT_IE6 HostnameVerifier works just like the STRICT one with one
     * minor variation:  the hostname can match against any of the CN's in the
     * server's certificate, not just the first one.  This behaviour is
     * identical to IE6's behaviour.
     */
    public final static SSLHostnameVerifier STRICT_IE6 =
        new AbstractVerifier() {
            @Override
            public final void check(final String[] host, final String[] cns,
                                    final String[] subjectAlts)
                throws SSLException {
                check(host, cns, subjectAlts, true, true);
            }

            @Override
            public final String toString() { return "STRICT_IE6"; }
        };

    /**
     * The ALLOW_ALL HostnameVerifier essentially turns hostname verification
     * off.  This implementation is a no-op, and never throws the SSLException.
     */
    public final static SSLHostnameVerifier ALLOW_ALL =
        new AbstractVerifier() {
            @Override
            public final void check(final String[] host, final String[] cns,
                                    final String[] subjectAlts) {
                // Allow everything - so never blowup.
            }

            @Override
            public final String toString() { return "ALLOW_ALL"; }
        };

    abstract class AbstractVerifier implements SSLHostnameVerifier {

        /**
         * Writes as SSLFactory logs as it is the only consumer of this verifier
         * class.
         */
        static final Logger LOG = LoggerFactory.getLogger(SSLFactory.class);

        /**
         * This contains a list of 2nd-level domains that aren't allowed to
         * have wildcards when combined with country-codes.
         * For example: [*.co.uk].
         * <p>
         * The [*.co.uk] problem is an interesting one.  Should we just hope
         * that CA's would never foolishly allow such a certificate to happen?
         * Looks like we're the only implementation guarding against this.
         * Firefox, Curl, Sun Java 1.4, 5, 6 don't bother with this check.
         */
        private final static String[] BAD_COUNTRY_2LDS =
            {"ac", "co", "com", "ed", "edu", "go", "gouv", "gov", "info",
                "lg", "ne", "net", "or", "org"};

        private final static String[] LOCALHOSTS = {"::1", "127.0.0.1",
            "localhost",
            "localhost.localdomain"};


        static {
            // Just in case developer forgot to manually sort the array.  :-)
            Arrays.sort(BAD_COUNTRY_2LDS);
            Arrays.sort(LOCALHOSTS);
        }

        protected AbstractVerifier() {}

        /**
         * The javax.net.ssl.HostnameVerifier contract.
         *
         * @param host    'hostname' we used to create our socket
         * @param session SSLSession with the remote server
         * @return true if the host matched the one in the certificate.
         */
        @Override
        public boolean verify(String host, SSLSession session) {
            try {
                Certificate[] certs = session.getPeerCertificates();
                X509Certificate x509 = (X509Certificate) certs[0];
                check(new String[]{host}, x509);
                return true;
            }
            catch (SSLException e) {
                return false;
            }
        }

        @Override
        public void check(String host, SSLSocket ssl) throws IOException {
            check(new String[]{host}, ssl);
        }

        @Override
        public void check(String host, X509Certificate cert)
            throws SSLException {
            check(new String[]{host}, cert);
        }

        @Override
        public void check(String host, String[] cns, String[] subjectAlts)
            throws SSLException {
            check(new String[]{host}, cns, subjectAlts);
        }

        @Override
        public void check(String host[], SSLSocket ssl)
            throws IOException {
            if (host == null) {
                throw new NullPointerException("host to verify is null");
            }

            SSLSession session = ssl.getSession();
            if (session == null) {
                // In our experience this only happens under IBM 1.4.x when
                // spurious (unrelated) certificates show up in the server'
                // chain.  Hopefully this will unearth the real problem:
                InputStream in = ssl.getInputStream();
                in.available();
                /*
                  If you're looking at the 2 lines of code above because
                  you're running into a problem, you probably have two
                  options:

                    #1.  Clean up the certificate chain that your server
                         is presenting (e.g. edit "/etc/apache2/server.crt"
                         or wherever it is your server's certificate chain
                         is defined).

                                               OR

                    #2.   Upgrade to an IBM 1.5.x or greater JVM, or switch
                          to a non-IBM JVM.
                */

                // If ssl.getInputStream().available() didn't cause an
                // exception, maybe at least now the session is available?
                session = ssl.getSession();
                if (session == null) {
                    // If it's still null, probably a startHandshake() will
                    // unearth the real problem.
                    ssl.startHandshake();

                    // Okay, if we still haven't managed to cause an exception,
                    // might as well go for the NPE.  Or maybe we're okay now?
                    session = ssl.getSession();
                }
            }
            Certificate[] certs;
            try {
                certs = session.getPeerCertificates();
            } catch (SSLPeerUnverifiedException spue) {
                InputStream in = ssl.getInputStream();
                in.available();
                // Didn't trigger anything interesting?  Okay, just throw
                // original.
                throw spue;
            }
            X509Certificate x509 = (X509Certificate) certs[0];
            check(host, x509);
        }

        @Override
        public void check(String[] host, X509Certificate cert)
            throws SSLException {
            String[] cns = Certificates.getCNs(cert);
            String[] subjectAlts = Certificates.getDNSSubjectAlts(cert);
            try {
                check(host, cns, subjectAlts);
            } catch (SSLException e) {
                LOG.error("Host check error {}", e);
                throw e;
            }
        }

        public void check(final String[] hosts, final String[] cns,
                          final String[] subjectAlts, final boolean ie6,
                          final boolean strictWithSubDomains)
            throws SSLException {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Hosts:{}, CNs:{} subjectAlts:{}, ie6:{}, " +
                    "strictWithSubDomains{}", Arrays.toString(hosts),
                    Arrays.toString(cns), Arrays.toString(subjectAlts), ie6,
                    strictWithSubDomains);
            }
            // Build up lists of allowed hosts For logging/debugging purposes.
            StringBuffer buf = new StringBuffer(32);
            buf.append('<');
            for (int i = 0; i < hosts.length; i++) {
                String h = hosts[i];
                h = h != null ? StringUtils.toLowerCase(h.trim()) : "";
                hosts[i] = h;
                if (i > 0) {
                    buf.append('/');
                }
                buf.append(h);
            }
            buf.append('>');
            String hostnames = buf.toString();
            // Build the list of names we're going to check.  Our DEFAULT and
            // STRICT implementations of the HostnameVerifier only use the
            // first CN provided.  All other CNs are ignored.
            // (Firefox, wget, curl, Sun Java 1.4, 5, 6 all work this way).
            final Set<String> names = new TreeSet<String>();
            if (cns != null && cns.length > 0 && cns[0] != null) {
                names.add(cns[0]);
                if (ie6) {
                    for (int i = 1; i < cns.length; i++) {
                        names.add(cns[i]);
                    }
                }
            }
            if (subjectAlts != null) {
                for (int i = 0; i < subjectAlts.length; i++) {
                    if (subjectAlts[i] != null) {
                        names.add(subjectAlts[i]);
                    }
                }
            }
            if (names.isEmpty()) {
                String msg = "Certificate for " + hosts[0] + " doesn't contain CN or DNS subjectAlt";
                throw new SSLException(msg);
            }

            // StringBuffer for building the error message.
            buf = new StringBuffer();

            boolean match = false;
            out:
            for (Iterator<String> it = names.iterator(); it.hasNext();) {
                // Don't trim the CN, though!
                final String cn = StringUtils.toLowerCase(it.next());
                // Store CN in StringBuffer in case we need to report an error.
            buf.append(" <")
            .append(cn)
            .append('>');
                if (it.hasNext()) {
                    buf.append(" OR");
                }

                // The CN better have at least two dots if it wants wildcard
                // action.  It also can't be [*.co.uk] or [*.co.jp] or
                // [*.org.uk], etc...
                boolean doWildcard = cn.startsWith("*.") &&
                                     cn.lastIndexOf('.') >= 0 &&
                                     !isIP4Address(cn) &&
                                     acceptableCountryWildcard(cn);

                for (int i = 0; i < hosts.length; i++) {
                    final String hostName =
                        StringUtils.toLowerCase(hosts[i].trim());
                    if (doWildcard) {
                        match = hostName.endsWith(cn.substring(1));
                        if (match && strictWithSubDomains) {
                            // If we're in strict mode, then [*.foo.com] is not
                            // allowed to match [a.b.foo.com]
                            match = countDots(hostName) == countDots(cn);
                        }
                    } else {
                        match = hostName.equals(cn);
                    }
                    if (match) {
                        break out;
                    }
                }
            }
            if (!match) {
                throw new SSLException("hostname in certificate didn't match: " + hostnames + " !=" + buf);
            }
        }

        public static boolean isIP4Address(final String cn) {
            boolean isIP4 = true;
            String tld = cn;
            int x = cn.lastIndexOf('.');
            // We only bother analyzing the characters after the final dot
            // in the name.
            if (x >= 0 && x + 1 < cn.length()) {
                tld = cn.substring(x + 1);
            }
            for (int i = 0; i < tld.length(); i++) {
                if (!Character.isDigit(tld.charAt(0))) {
                    isIP4 = false;
                    break;
                }
            }
            return isIP4;
        }

        public static boolean acceptableCountryWildcard(final String cn) {
            int cnLen = cn.length();
            if (cnLen >= 7 && cnLen <= 9) {
                // Look for the '.' in the 3rd-last position:
                if (cn.charAt(cnLen - 3) == '.') {
                    // Trim off the [*.] and the [.XX].
                    String s = cn.substring(2, cnLen - 3);
                    // And test against the sorted array of bad 2lds:
                    int x = Arrays.binarySearch(BAD_COUNTRY_2LDS, s);
                    return x < 0;
                }
            }
            return true;
        }

        public static boolean isLocalhost(String host) {
            host = host != null ? StringUtils.toLowerCase(host.trim()) : "";
            if (host.startsWith("::1")) {
                int x = host.lastIndexOf('%');
                if (x >= 0) {
                    host = host.substring(0, x);
                }
            }
            int x = Arrays.binarySearch(LOCALHOSTS, host);
            return x >= 0;
        }

        /**
         * Counts the number of dots "." in a string.
         *
         * @param s string to count dots from
         * @return number of dots
         */
        public static int countDots(final String s) {
            int count = 0;
            for (int i = 0; i < s.length(); i++) {
                if (s.charAt(i) == '.') {
                    count++;
                }
            }
            return count;
        }
    }

    static class Certificates {
      public static String[] getCNs(X509Certificate cert) {
        final List<String> cnList = new LinkedList<String>();
        /*
          Sebastian Hauer's original StrictSSLProtocolSocketFactory used
          getName() and had the following comment:

             Parses a X.500 distinguished name for the value of the
             "Common Name" field.  This is done a bit sloppy right
             now and should probably be done a bit more according to
             <code>RFC 2253</code>.

           I've noticed that toString() seems to do a better job than
           getName() on these X500Principal objects, so I'm hoping that
           addresses Sebastian's concern.

           For example, getName() gives me this:
           1.2.840.113549.1.9.1=#16166a756c6975736461766965734063756362632e636f6d

           whereas toString() gives me this:
           EMAILADDRESS=juliusdavies@cucbc.com

           Looks like toString() even works with non-ascii domain names!
           I tested it with "&#x82b1;&#x5b50;.co.jp" and it worked fine.
          */
        String subjectPrincipal = cert.getSubjectX500Principal().toString();
        StringTokenizer st = new StringTokenizer(subjectPrincipal, ",");
        while (st.hasMoreTokens()) {
            String tok = st.nextToken();
            int x = tok.indexOf("CN=");
            if (x >= 0) {
                cnList.add(tok.substring(x + 3));
            }
        }
        if (!cnList.isEmpty()) {
            String[] cns = new String[cnList.size()];
            cnList.toArray(cns);
            return cns;
        } else {
            return null;
        }
      }


      /**
       * Extracts the array of SubjectAlt DNS names from an X509Certificate.
       * Returns null if there aren't any.
       * <p>
       * Note:  Java doesn't appear able to extract international characters
       * from the SubjectAlts.  It can only extract international characters
       * from the CN field.
       * <p>
       * (Or maybe the version of OpenSSL I'm using to test isn't storing the
       * international characters correctly in the SubjectAlts?).
       *
       * @param cert X509Certificate
       * @return Array of SubjectALT DNS names stored in the certificate.
       */
      public static String[] getDNSSubjectAlts(X509Certificate cert) {
          final List<String> subjectAltList = new LinkedList<String>();
          Collection<List<?>> c = null;
          try {
              c = cert.getSubjectAlternativeNames();
          }
          catch (CertificateParsingException cpe) {
              // Should probably log.debug() this?
              cpe.printStackTrace();
          }
          if (c != null) {
              Iterator<List<?>> it = c.iterator();
              while (it.hasNext()) {
                  List<?> list = it.next();
                  int type = ((Integer) list.get(0)).intValue();
                  // If type is 2, then we've got a dNSName
                  if (type == 2) {
                      String s = (String) list.get(1);
                      subjectAltList.add(s);
                  }
              }
          }
          if (!subjectAltList.isEmpty()) {
              String[] subjectAlts = new String[subjectAltList.size()];
              subjectAltList.toArray(subjectAlts);
              return subjectAlts;
          } else {
              return null;
          }
      }
    }

}
