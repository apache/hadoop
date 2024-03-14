package org.apache.hadoop.fs.azurebfs.services.kac;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.http.HttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;

public class KeepAliveCache extends HashMap<KeepAliveCache.KeepAliveKey, KeepAliveCache.ClientVector>
    implements Runnable{

  private Thread thread;
  private final static String HTTP_MAX_CONN_SYS_PROP = "http.maxConnections";
  private final static Integer DEFAULT_MAX_CONN_SYS_PROP = 5;

  private final int maxConn;

  private KeepAliveCache() {
    thread = new Thread(this);
    thread.start();
    String sysPropMaxConn = System.getProperty(HTTP_MAX_CONN_SYS_PROP);
    if (sysPropMaxConn == null) {
      maxConn = DEFAULT_MAX_CONN_SYS_PROP;
    } else {
      maxConn = Integer.parseInt(sysPropMaxConn);
    }
  }
  public static KeepAliveCache INSTANCE = new KeepAliveCache();

  private int getKacSize() {
    return INSTANCE.maxConn;
  }

  @Override
  public void run() {
    try {
      Thread.sleep(5_000L);
      synchronized (this) {
        /* Remove all unused HttpClients.  Starting from the
         * bottom of the stack (the least-recently used first).
         * REMIND: It'd be nice to not remove *all* connections
         * that aren't presently in use.  One could have been added
         * a second ago that's still perfectly valid, and we're
         * needlessly axing it.  But it's not clear how to do this
         * cleanly, and doing it right may be more trouble than it's
         * worth.
         */

        long currentTime = System.currentTimeMillis();

        ArrayList<KeepAliveKey> keysToRemove
            = new ArrayList<KeepAliveKey>();

        for (KeepAliveKey key : keySet()) {
          ClientVector v = get(key);
          synchronized (v) {
            int i;

            for (i = 0; i < v.size(); i++) {
              KeepAliveEntry e = v.elementAt(i);
              if ((currentTime - e.idleStartTime) > v.nap) {
                HttpClientConnection hc = e.httpClientConnection;
                hc.close();
              } else {
                break;
              }
            }
            v.subList(0, i).clear();

            if (v.size() == 0) {
              keysToRemove.add(key);
            }
          }
        }

        for (KeepAliveKey key : keysToRemove) {
          removeVector(key);
        }
      }
    } catch (Exception ex) {}
  }

  synchronized void removeVector(KeepAliveKey k) {
    super.remove(k);
  }

  public synchronized void put(final HttpRoute httpRoute, final HttpClientConnection httpClientConnection) {
    KeepAliveKey key = new KeepAliveKey(httpRoute);
    ClientVector v = super.get(key);
    if(v == null) {
      v= new ClientVector(5_000);
      v.put(httpClientConnection);
      super.put(key, v);
    } else {
      v.put(httpClientConnection);
    }
  }

  public synchronized HttpClientConnection get(HttpRoute httpRoute) throws IOException {

    KeepAliveKey key = new KeepAliveKey(httpRoute);
    ClientVector v = super.get(key);
    if (v == null) { // nothing in cache yet
      return null;
    }
    return v.get();
  }

  class ClientVector extends java.util.Stack<KeepAliveEntry> {
    private static final long serialVersionUID = -8680532108106489459L;

    // sleep time in milliseconds, before cache clear
    int nap;



    ClientVector (int nap) {
      this.nap = nap;
    }

    synchronized HttpClientConnection get() throws IOException {
      if (empty()) {
        return null;
      } else {
        // Loop until we find a connection that has not timed out
        HttpClientConnection hc = null;
        long currentTime = System.currentTimeMillis();
        do {
          KeepAliveEntry e = pop();
          if ((currentTime - e.idleStartTime) > nap) {
            e.httpClientConnection.close();
          } else {
            hc = e.httpClientConnection;
          }
        } while ((hc== null) && (!empty()));
        return hc;
      }
    }

    /* return a still valid, unused HttpClient */
    synchronized void put(HttpClientConnection h) {
      if (size() >= getKacSize()) {
        try {h.close();} catch (Exception e) {}
        ;
        return;
      }
      push(new KeepAliveEntry(h, System.currentTimeMillis()));
    }

    /*
     * Do not serialize this class!
     */
    private void writeObject(java.io.ObjectOutputStream stream)
        throws IOException {
      throw new NotSerializableException();
    }

    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
      throw new NotSerializableException();
    }
  }


  class KeepAliveKey {
    private final HttpRoute httpRoute;


    public KeepAliveKey(HttpRoute httpRoute) {
      this.httpRoute = httpRoute;
    }

    /**
     * Determine whether or not two objects of this type are equal
     */
    @Override
    public boolean equals(Object obj) {
      return obj instanceof KeepAliveKey && httpRoute.getTargetHost().getHostName().equals(((KeepAliveKey) obj).httpRoute.getTargetHost().getHostName());
    }

    /**
     * The hashCode() for this object is the string hashCode() of
     * concatenation of the protocol, host name and port.
     */
    @Override
    public int hashCode() {
      String str = httpRoute.getTargetHost().getHostName() + ":" + httpRoute.getTargetHost().getPort();
      return str.hashCode();
    }
  }

  class KeepAliveEntry {
    HttpClientConnection httpClientConnection;
    long idleStartTime;

    KeepAliveEntry(HttpClientConnection hc, long idleStartTime) {
      this.httpClientConnection = hc;
      this.idleStartTime = idleStartTime;
    }
  }
}
