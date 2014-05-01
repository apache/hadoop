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

#ifndef HADOOP_CORE_RPC_CONNECTION_H
#define HADOOP_CORE_RPC_CONNECTION_H

#include "common/tree.h"
#include "rpc/client_id.h"

#include <stdint.h>
#include <uv.h>

struct hrpc_call;

#define MAX_CUR_WRITES 2
#define READLEN_BUF_LEN 4

enum hrpc_conn_write_state {
    /**
     * The state when we're still calling connect(2).
     */
    HRPC_CONN_WRITE_CONNECTING,

    /**
     * The write state where we're sending the frame, IPC header, etc.
     * TODO: implement SASL and its associated states.
     */
    HRPC_CONN_WRITE_IPC_HEADER,

    /**
     * The write state where we're writing the RpcRequestHeaderProto and
     * request payload.
     */
    HRPC_CONN_WRITE_PAYLOAD,

    /**
     * The write state where we have nothing to write.
     */
    HRPC_CONN_WRITE_IDLE,

    /**
     * Closed state.
     */
    HRPC_CONN_WRITE_CLOSED,
};

enum hrpc_conn_read_state {
    /**
     * The read state in which we don't expect read callbacks.
     * Generally, we're only in this state when the connection itself has not
     * been established.
     */
    HRPC_CONN_UNREADABLE = 0,

    /**
     * The read state in which we're reading the 4-byte length prefix.
     */
    HRPC_CONN_READ_LEN,

    /**
     * The read state in which we're reading the message body.
     */
    HRPC_CONN_READ_BODY,

    /**
     * Closed state.
     */
    HRPC_CONN_READ_CLOSED,
};

struct hrpc_conn_reader {
    enum hrpc_conn_read_state state; //!< Current read state.
    uint8_t body_len_buf[READLEN_BUF_LEN];//!< The buffer for body length.
    int32_t body_len;            //!< Body length to read.
    int32_t off;                 //!< Current offset.
    char *body;                  //!< malloc'ed message body we're reading.
};

struct hrpc_conn_writer {
    enum hrpc_conn_write_state state;//!< Current write state.
    uv_write_t write_req;
    uv_buf_t cur_writes[MAX_CUR_WRITES];
};

/**
 * A Hadoop connection.
 *
 * This object manages the TCP connection with the remote.
 * Note that we can read and write from the remote simultaneously;
 * that's why write_state and read_state are separate.
 */
struct hrpc_conn {
    RB_ENTRY(hrpc_conn) entry;

    /**
     * The reactor that owns this connection.
     */
    struct hrpc_reactor *reactor;

    /**
     * The call we're handling.
     */
    struct hrpc_call *call;

    /**
     * The remote address we're connected to.
     */
    struct sockaddr_in remote;         

    /**
     * This connection's TCP stream.
     */
    uv_tcp_t stream;

    /**
     * The Hadoop protocol we're talking to, such as
     * org.apache.hadoop.hdfs.protocol.ClientProtocol.  Malloc'ed.
     */
    char *protocol;

    /**
     * The client ID we used when establishing the connection.
     */
    struct hrpc_client_id client_id;  

    /**
     * The pending connection request, if one is pending.
     */
    uv_connect_t conn_req;

    struct hrpc_conn_writer writer;
    struct hrpc_conn_reader reader;
};

/**
 * Create a new hrpc_conn to the given remote, using bind, connect, etc.
 *
 * @param reactor       The reactor that the connection will be associated
 *                          with.
 * @param call          The call to make.  The connection will take ownership
 *                          of this call.  If the connection fails, the call
 *                          will be given a failure callback.
 * @param out           (out param) On success, the new connection.
 *
 * @return              NULL on success; the error otherwise.
 */
struct hadoop_err *hrpc_conn_create_outbound(struct hrpc_reactor *reactor,
                                    struct hrpc_call *call,
                                    struct hrpc_conn **out);

/**
 * Start an outbound call on this connection.
 *
 * @param conn          The connection.
 * @param call          The call.
 */
void hrpc_conn_start_outbound(struct hrpc_conn *conn, struct hrpc_call *call);

/**
 * Compare two hadoop connection objects.
 *
 * Comparison is done lexicographically by:
 *    - IP address.
 *    - Port.
 *    - Whether the connection is in use.
 *    - Memory Address.
 *
 * By doing the comparison this way, we make it easy to find the first idle
 * connection to a given remote, or quickly determine that there is not one.
 * We also allow multiple connections to the same address.
 *
 * @param a             The first connection object.
 * @param b             The second connection object.
 *
 * @return              negative if a < b; positive if a > b; 0 otherwise.
 */
int hrpc_conn_compare(const struct hrpc_conn *a,
                      const struct hrpc_conn *b);

/**
 * Determine if a connection is usable for a given address and protocol.
 *
 * @param conn          The connection.
 * @param addr          The address.
 * @param protocol      The protocol.
 *
 * @return              1 if the connection is usable; 0 if not.
 */
int hrpc_conn_usable(const struct hrpc_conn *conn,
                      const struct sockaddr_in *addr, const char *protocol);

/**
 * Destroy a connection.
 *
 * @param conn          The connection to destroy.
 * @param err           The error.  This will be delivered to any callback that
 *                          the connection owns.
 */
void hrpc_conn_destroy(struct hrpc_conn *conn, struct hadoop_err *err);

#endif

// vim: ts=4:sw=4:tw=79:et
