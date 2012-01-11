package org.apache.hadoop.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This is the rpc payload header. It is sent with every rpc call
 * <pre>
 * The format of RPC call is as follows:
 * +---------------------------------------------------+
 * |  Rpc length in bytes (header + payload length)    |
 * +---------------------------------------------------+
 * |      Rpc Header       |       Rpc Payload         |
 * +---------------------------------------------------+
 * 
 * The format of Rpc Header is:
 * +----------------------------------+
 * |  RpcKind (1 bytes)               |      
 * +----------------------------------+
 * |  RpcPayloadOperation (1 bytes)   |      
 * +----------------------------------+
 * |  Call ID (4 bytes)               |      
 * +----------------------------------+
 * 
 * {@link RpcKind} determines the type of serialization used for Rpc Payload.
 * </pre>
 * <p>
 * <b>Note this header does NOT have its own version number, 
 * it used the version number from the connection header. </b>
 */
public class RpcPayloadHeader implements Writable {
  public enum RpcPayloadOperation {
    RPC_FINAL_PAYLOAD ((short)1),
    RPC_CONTINUATION_PAYLOAD ((short)2), // not implemented yet
    RPC_CLOSE_CONNECTION ((short)3);     // close the rpc connection
    
    private final short code;
    private static final short FIRST_INDEX = RPC_FINAL_PAYLOAD.code;
    RpcPayloadOperation(short val) {
      this.code = val;
    }
    
    public void write(DataOutput out) throws IOException {  
      out.writeByte(code);
    }
    
    static RpcPayloadOperation readFields(DataInput in) throws IOException {
      short inValue = in.readByte();
      return RpcPayloadOperation.values()[inValue - FIRST_INDEX];
    }
  }
  
  public enum RpcKind {
    RPC_BUILTIN ((short ) 1),  // Used for built in calls
    RPC_WRITABLE ((short ) 2),
    RPC_PROTOCOL_BUFFER ((short)3), 
    RPC_AVRO ((short)4);
    
    private final short value;
    private static final short FIRST_INDEX = RPC_BUILTIN.value;
    RpcKind(short val) {
      this.value = val;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeByte(value);
    }
    
    static RpcKind readFields(DataInput in) throws IOException {
      short inValue = in.readByte();
      return RpcKind.values()[inValue - FIRST_INDEX];
    }  
  }
  
  private RpcKind kind;
  private RpcPayloadOperation operation;
  private int callId;
  
  public RpcPayloadHeader() {
    kind = RpcKind.RPC_WRITABLE;
    operation = RpcPayloadOperation.RPC_CLOSE_CONNECTION;
  }
  
  public RpcPayloadHeader(RpcKind kind, RpcPayloadOperation op, int callId) {
    this.kind  = kind;
    this.operation = op;
    this.callId = callId;
  }
  
  int getCallId() {
    return callId;
  }
  
  RpcKind getkind() {
    return kind;
  }
  
  RpcPayloadOperation getOperation() {
    return operation;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    kind.write(out);
    operation.write(out);
    out.writeInt(callId); 
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    kind = RpcKind.readFields(in);
    operation = RpcPayloadOperation.readFields(in);
    this.callId = in.readInt();
  }
}
