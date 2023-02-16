package org.apache.hadoop.fs.qiniu.kodo.client.batch;

public final class Product<V, E extends Exception> {
    private final V value;
    private final boolean isEOF;
    private final E exception;

    private Product(V value, boolean isEOF, E exception) {
        this.value = value;
        this.isEOF = isEOF;
        this.exception = exception;
    }

    public V getValue() {
        return value;
    }

    public boolean isEOF() {
        return isEOF;
    }

    public E getException() {
        return exception;
    }

    public boolean hasValue() {
        return value != null;
    }

    public boolean hasException() {
        return exception != null;
    }

    public static <V, E extends Exception> Product<V, E> wrapEOF() {
        return new Product<>(
                null, true, null);
    }

    public static <V, E extends Exception> Product<V, E> wrapData(V e) {
        return new Product<>(e, false, null);
    }

    public static <V, E extends Exception> Product<V, E> wrapException(E e) {
        return new Product<>(null, false, e);
    }

    @Override
    public String toString() {
        return "Product{" +
                "value=" + value +
                ", isEOF=" + isEOF +
                ", exception=" + exception +
                '}';
    }
}
