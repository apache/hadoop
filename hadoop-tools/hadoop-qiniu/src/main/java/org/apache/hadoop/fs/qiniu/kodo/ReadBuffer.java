package org.apache.hadoop.fs.qiniu.kodo;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadBuffer {
    enum STATUS {
        INIT, SUCCESS, ERROR
    }

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition readyCondition = lock.newCondition();

    private final byte[] buffer;
    private STATUS status;
    private final long byteStart;
    private final long byteEnd;

    public ReadBuffer(long byteStart, long byteEnd) {
        this.buffer = new byte[(int) (byteEnd - byteStart) + 1];

        this.status = STATUS.INIT;
        this.byteStart = byteStart;
        this.byteEnd = byteEnd;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void await(STATUS waitStatus) throws InterruptedException {
        while (this.status == waitStatus) {
            readyCondition.await();
        }
    }

    public void signalAll() {
        readyCondition.signalAll();
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public STATUS getStatus() {
        return status;
    }

    public void setStatus(STATUS status) {
        this.status = status;
    }

    public long getByteStart() {
        return byteStart;
    }

    public long getByteEnd() {
        return byteEnd;
    }
}
