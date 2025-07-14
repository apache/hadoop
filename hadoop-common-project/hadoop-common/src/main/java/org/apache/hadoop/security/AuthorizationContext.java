package org.apache.hadoop.security;

/**
 * Utility for managing a thread-local authorization header for RPC calls.
 */
public final class AuthorizationContext {
    private static final ThreadLocal<byte[]> AUTH_HEADER = new ThreadLocal<>();

    private AuthorizationContext() {}

    public static void setCurrentAuthorizationHeader(byte[] header) {
        AUTH_HEADER.set(header);
    }

    public static byte[] getCurrentAuthorizationHeader() {
        return AUTH_HEADER.get();
    }

    public static void clear() {
        AUTH_HEADER.remove();
    }
}