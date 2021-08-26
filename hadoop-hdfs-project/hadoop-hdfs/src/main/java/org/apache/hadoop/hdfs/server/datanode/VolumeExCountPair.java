package org.apache.hadoop.hdfs.server.datanode;

/**
 * Record volume's IOException total counts and update timestamp
 */
public class VolumeExCountPair {

    private long prevTs;
    private long IoExceptionCnt;

    private VolumeExCountPair() {
    }

    public VolumeExCountPair(long prevTimeStamp, long IoExceptionCnt) {
        this.prevTs = prevTimeStamp;
        this.IoExceptionCnt = IoExceptionCnt;
    }

    public void setNewPair(long now, long curExCnt) {
        setPrevTs(now);
        setIoExceptionCnt(curExCnt);
    }

    public VolumeExCountPair getPair() {
        return new VolumeExCountPair(prevTs, IoExceptionCnt);
    }

    public void setPrevTs(long prevTs) {
        this.prevTs = prevTs;
    }

    public void setIoExceptionCnt(long ioExceptionCnt) {
        IoExceptionCnt = ioExceptionCnt;
    }

    public long getPrevTs() {
        return prevTs;
    }

    public long getIoExceptionCnt() {
        return IoExceptionCnt;
    }
}