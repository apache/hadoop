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

#include "common/hadoop_err.h"
#include "common/net.h"
#include "common/string.h"
#include "protobuf/IpcConnectionContext.pb-c.h.s"
#include "protobuf/ProtobufRpcEngine.pb-c.h.s"
#include "protobuf/RpcHeader.pb-c.h.s"
#include "rpc/call.h"
#include "rpc/conn.h"
#include "rpc/messenger.h"
#include "rpc/proxy.h"
#include "rpc/reactor.h"
#include "rpc/varint.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define conn_log_warn(conn, fmt, ...) \
    fprintf(stderr, "WARN: conn %p (reactor %p): " fmt, \
            conn, conn->reactor, __VA_ARGS__)
#define conn_log_info(conn, fmt, ...) \
    fprintf(stderr, "INFO: conn %p (reactor %p): " fmt, \
            conn, conn->reactor, __VA_ARGS__)
#define conn_log_debug(conn, fmt, ...) \
    fprintf(stderr, "DEBUG: conn %p (reactor %p): " fmt, \
            conn, conn->reactor, __VA_ARGS__)

/**
 * The maximum length that we'll allocate to hold a response from the server.
 * This number includes the response header.
 */
#define MAX_READER_BODY_LEN (64 * 1024 * 1024)

static const uint8_t FRAME[] = {
    0x68, 0x72, 0x70, 0x63, // "hrpc"
    0x09,                   // version
    0x00,                   // service class
    0x00                    // auth
};

#define FRAME_LEN sizeof(FRAME)

#define CONNECTION_CONTEXT_CALL_ID (-3)

static void conn_read_cb(uv_stream_t *stream, ssize_t nread,
                         const uv_buf_t* buf);
static void hrpc_conn_read_alloc(uv_handle_t *handle,
                        size_t suggested_size, uv_buf_t *buf);

static const char *conn_write_state_str(enum hrpc_conn_write_state state)
{
    switch (state) {
    case HRPC_CONN_WRITE_CONNECTING:
        return "HRPC_CONN_WRITE_CONNECTING";
    case HRPC_CONN_WRITE_IPC_HEADER:
        return "HRPC_CONN_WRITE_IPC_HEADER";
    case HRPC_CONN_WRITE_PAYLOAD:
        return "HRPC_CONN_WRITE_PAYLOAD";
    case HRPC_CONN_WRITE_IDLE:
        return "HRPC_CONN_WRITE_IDLE";
    case HRPC_CONN_WRITE_CLOSED:
        return "HRPC_CONN_WRITE_CLOSED";
    default:
        return "(unknown)";
    }
};

static void free_write_bufs(struct hrpc_conn *conn)
{
    int i;
    uv_buf_t *cur_writes = conn->writer.cur_writes;

    for (i = 0; i < MAX_CUR_WRITES; i++) {
        free(cur_writes[i].base);
        cur_writes[i].base = NULL;
        cur_writes[i].len = 0;
    }
}

static struct hadoop_err *conn_setup_ipc_header(struct hrpc_conn *conn)
{
    struct hrpc_conn_writer *writer = &conn->writer;
    IpcConnectionContextProto ipc_ctx = IPC_CONNECTION_CONTEXT_PROTO__INIT;
    RpcRequestHeaderProto rpc_req_header = RPC_REQUEST_HEADER_PROTO__INIT;
    UserInformationProto userinfo = USER_INFORMATION_PROTO__INIT;
    int32_t cset_len, buf_len;
    int32_t ipc_ctx_len, rpc_req_header_len, off = 0;
    uint8_t *buf;
    struct hadoop_err *err;

    rpc_req_header.has_rpckind = 1;
    rpc_req_header.rpckind = RPC_PROTOCOL_BUFFER;
    rpc_req_header.has_rpcop = 1;
    rpc_req_header.rpcop = RPC_FINAL_PACKET;
    rpc_req_header.callid = CONNECTION_CONTEXT_CALL_ID;
    rpc_req_header.clientid.data = conn->client_id.buf;
    rpc_req_header.clientid.len = HRPC_CLIENT_ID_LEN;
    rpc_req_header.has_retrycount = 0;
    rpc_req_header_len =
        rpc_request_header_proto__get_packed_size(&rpc_req_header);
    userinfo.effectiveuser = conn->username;
    userinfo.realuser = NULL;
    ipc_ctx.userinfo = &userinfo;
    ipc_ctx.protocol = conn->protocol;
    ipc_ctx_len = ipc_connection_context_proto__get_packed_size(&ipc_ctx);

    cset_len = varint32_size(rpc_req_header_len) + rpc_req_header_len +
        varint32_size(ipc_ctx_len) + ipc_ctx_len;
    buf_len = FRAME_LEN + sizeof(uint32_t) + cset_len;
    buf = malloc(buf_len);
    if (!buf) {
        err = hadoop_lerr_alloc(ENOMEM, "conn_setup_ipc_header: "
                    "failed to allocate %d bytes", buf_len);
        return err;
    }
    memcpy(buf, FRAME, FRAME_LEN);
    off += FRAME_LEN;
    be32_encode(cset_len, buf + off);
    off += sizeof(uint32_t);
    varint32_encode(rpc_req_header_len, buf, buf_len, &off);
    rpc_request_header_proto__pack(&rpc_req_header, buf + off);
    off += rpc_req_header_len;
    varint32_encode(ipc_ctx_len, buf, buf_len, &off);
    ipc_connection_context_proto__pack(&ipc_ctx, buf + off);
    free_write_bufs(conn);
    writer->cur_writes[0].base = (char*)buf;
    writer->cur_writes[0].len = buf_len;
    return NULL;
}

static struct hadoop_err *hrpc_conn_setup_payload(struct hrpc_conn *conn)
{
    struct hrpc_conn_writer *writer = &conn->writer;
    struct hrpc_call *call = conn->call;
    RpcRequestHeaderProto rpc_req_header = RPC_REQUEST_HEADER_PROTO__INIT;
    int32_t rpc_req_header_len, header_buf_len, total_len, off = 0;
    int64_t total_len64;
    uint8_t *header_buf;
    struct hadoop_err *err;

    rpc_req_header.has_rpckind = 1;
    rpc_req_header.rpckind = RPC_PROTOCOL_BUFFER;
    rpc_req_header.has_rpcop = 1;
    rpc_req_header.rpcop = RPC_FINAL_PACKET;
    rpc_req_header.callid = conn->call_id;
    rpc_req_header.clientid.data = conn->client_id.buf;
    rpc_req_header.clientid.len = HRPC_CLIENT_ID_LEN;
    rpc_req_header.has_retrycount = 0;
    rpc_req_header_len =
        rpc_request_header_proto__get_packed_size(&rpc_req_header);

    total_len64 = varint32_size(rpc_req_header_len);
    total_len64 += rpc_req_header_len;
    total_len64 += call->payload.len;
    if (total_len64 > MAX_READER_BODY_LEN) {
        err = hadoop_lerr_alloc(EINVAL, "hrpc_conn_setup_payload: "
                "can't send a payload of length %" PRId64 ".  The maximum "
                "payload length is %d", total_len64, MAX_READER_BODY_LEN);
        return err;
    }
    total_len = (int32_t)total_len64;
    header_buf_len = total_len - call->payload.len + sizeof(uint32_t);
    header_buf = malloc(header_buf_len);
    if (!header_buf) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_conn_setup_payload: "
                    "failed to allocate %d bytes for header.", header_buf_len);
        return err;
    }
    be32_encode(total_len, header_buf + off);
    off += sizeof(uint32_t);
    varint32_encode(rpc_req_header_len, header_buf, header_buf_len, &off);
    rpc_request_header_proto__pack(&rpc_req_header, header_buf + off);
    writer->cur_writes[0].base = (char*)header_buf;
    writer->cur_writes[0].len = header_buf_len;
    writer->cur_writes[1].base = call->payload.base;
    writer->cur_writes[1].len = call->payload.len;
    call->payload.base = NULL;
    call->payload.len = 0;
    return NULL;
}

static void conn_write_cb(uv_write_t* req, int status)
{
    struct hrpc_conn *conn = req->data;
    struct hrpc_conn_writer *writer = &conn->writer;
    struct hrpc_conn_reader *reader = &conn->reader;
    struct hadoop_err *err;
    int res;

    if (status) {
        err = hadoop_uverr_alloc(status,
                "conn_write_cb got error"); 
        hrpc_conn_destroy(conn, err);
        return;
    }
    switch (writer->state) {
    case HRPC_CONN_WRITE_IPC_HEADER:
        free_write_bufs(conn);
        writer->state = HRPC_CONN_WRITE_PAYLOAD;
        err = hrpc_conn_setup_payload(conn);
        if (err) {
            hrpc_conn_destroy(conn, err);
            return;
        }
        writer->write_req.data = conn;
        res = uv_write(&writer->write_req, (uv_stream_t*)&conn->stream, 
                     writer->cur_writes, 2, conn_write_cb);
        if (res) {
            err = hadoop_uverr_alloc(res,
                    "failed to call uv_write on payload");
            hrpc_conn_destroy(conn, err);
            return;
        }
        break;
    case HRPC_CONN_WRITE_PAYLOAD:
        conn_log_debug(conn, "%s", "conn_write_cb: finished writing payload.  "
                      "Now waiting for response.\n");
        free_write_bufs(conn);
        writer->state = HRPC_CONN_WRITE_IDLE;
        reader->state = HRPC_CONN_READ_LEN;
        conn->stream.data = conn;
        res = uv_read_start((uv_stream_t*)&conn->stream, hrpc_conn_read_alloc,
                          conn_read_cb);
        if (res) {
            err = hadoop_uverr_alloc(res, "uv_read_start failed");
            hrpc_conn_destroy(conn, err);
            return;
        }
        break;
    default:
        conn_log_warn(conn, "conn_write_cb: got an unexpected write "
                      "event while in %s state.\n",
                      conn_write_state_str(writer->state));
        return;
    }
}

static void conn_start_outbound(struct hrpc_conn *conn)
{
    struct hadoop_err *err = NULL;
    struct hrpc_conn_writer *writer = &conn->writer;
    int res;

    // Get next call ID to use.
    if (conn->call_id >= 0x7fffffff) {
        conn->call_id = 1;
    } else {
        conn->call_id++;
    }
    writer->state = HRPC_CONN_WRITE_IPC_HEADER;
    err = conn_setup_ipc_header(conn);
    if (err) {
        hrpc_conn_destroy(conn, err);
        return;
    }
    writer->write_req.data = conn;
    res = uv_write(&writer->write_req, (uv_stream_t*)&conn->stream,
                 writer->cur_writes, 1, conn_write_cb);
    if (res) {
        err = hadoop_uverr_alloc(res,
                "failed to call uv_write on ipc_header_buf");
        hrpc_conn_destroy(conn, err);
        return;
    }
}

void hrpc_conn_start_outbound(struct hrpc_conn *conn, struct hrpc_call *call)
{
    conn->call = call;
    conn_start_outbound(conn);
}

static void conn_connect_cb(uv_connect_t *req, int status)
{
    struct hrpc_conn *conn = req->data;
    struct hadoop_err *err = NULL;
    struct hrpc_conn_writer *writer = &conn->writer;

    if (status) {
        err = hadoop_uverr_alloc(status, "uv_tcp_connect failed");
        hrpc_conn_destroy(conn, err);
        return;
    }
    if (writer->state != HRPC_CONN_WRITE_CONNECTING) {
        err = hadoop_lerr_alloc(EINVAL,
                "got conn_connect_cb, but connection was not in "
                "state HRPC_CONN_WRITE_CONNECTING.  state = %d",
                writer->state);
        hrpc_conn_destroy(conn, err);
        return;
    }
    conn_start_outbound(conn);
}

struct hadoop_err *hrpc_conn_create_outbound(struct hrpc_reactor *reactor,
                                    struct hrpc_call *call,
                                    struct hrpc_conn **out)
{
    struct hadoop_err *err = NULL;
    struct hrpc_conn *conn = NULL;
    int res, tcp_init = 0;

    conn = calloc(1, sizeof(struct hrpc_conn));
    if (!conn) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_conn_create_outbound: OOM");
        goto done;
    }
    conn->reactor = reactor;
    conn->call = call;
    conn->remote = call->remote;
    conn->protocol = strdup(call->protocol);
    conn->username = strdup(call->username);
    if ((!conn->protocol) || (!conn->username)) {
        err = hadoop_lerr_alloc(ENOMEM, "hrpc_conn_create_outbound: OOM");
        goto done;
    }
    hrpc_client_id_generate_rand(&conn->client_id);
    res = uv_tcp_init(&reactor->loop, &conn->stream);
    if (res) {
        err = hadoop_uverr_alloc(res,
                "hrpc_conn_create_outbound: uv_tcp_init failed");
        goto done;
    }
    tcp_init = 1;
    conn->writer.state = HRPC_CONN_WRITE_CONNECTING;
    conn->reader.state = HRPC_CONN_UNREADABLE;
    conn->conn_req.data = conn;
    res = uv_tcp_connect(&conn->conn_req, &conn->stream,
            (struct sockaddr*)&conn->remote, conn_connect_cb);
    if (res) {
        char remote_str[64] = { 0 };
        err = hadoop_uverr_alloc(res,
                "hrpc_conn_create_outbound: uv_tcp_connect(%s) failed",
                net_ipv4_name_and_port(&conn->remote, remote_str,
                                       sizeof(remote_str)));
        goto done;
    }

done:
    if (err) {
        if (conn) {
            free(conn->protocol);
            free(conn->username);
            if (tcp_init) {
                uv_close((uv_handle_t*)&conn->stream, NULL);
            }
            free(conn);
        }
        return err;
    }
    *out = conn;
    return NULL;
}

int hrpc_conn_compare(const struct hrpc_conn *a, const struct hrpc_conn *b)
{
    int proto_cmp, username_cmp, a_active, b_active;

    // Big-endian versus little-endian doesn't matter here.
    // We just want a consistent ordering on the same machine.
    if (a->remote.sin_addr.s_addr < b->remote.sin_addr.s_addr)
        return -1;
    else if (a->remote.sin_addr.s_addr > b->remote.sin_addr.s_addr)
        return 1;
    else if (a->remote.sin_port < b->remote.sin_port)
        return -1;
    else if (a->remote.sin_port > b->remote.sin_port)
        return 1;
    // Compare protocol name.
    proto_cmp = strcmp(a->protocol, b->protocol);
    if (proto_cmp != 0)
        return proto_cmp;
    // Compare username.
    username_cmp = strcmp(a->username, b->username);
    if (username_cmp != 0)
        return username_cmp;
    // Make the inactive connections sort before the active ones.
    a_active = !!a->call;
    b_active = !!b->call;
    if (a_active < b_active) 
        return -1;
    else if (a_active > b_active) 
        return 1;
    // Compare pointer identity, so that no two distinct connections are
    // ever identical.
    else if (a < b)
        return -1;
    else if (a > b)
        return 1;
    return 0;
}

int hrpc_conn_usable(const struct hrpc_conn *conn,
            const struct sockaddr_in *addr,
            const char *protocol, const char *username)
{
    if (conn->remote.sin_addr.s_addr != addr->sin_addr.s_addr)
        return 0;
    else if (conn->remote.sin_port != addr->sin_port)
        return 0;
    else if (strcmp(conn->protocol, protocol))
        return 0;
    else if (strcmp(conn->username, username))
        return 0;
    return 1;
}

static void free_read_bufs(struct hrpc_conn *conn)
{
    free(conn->reader.body);
    conn->reader.body = NULL;
    conn->reader.body_len = 0;
    conn->reader.off = 0;
}

static struct hadoop_err *conn_deliver_resp(struct hrpc_conn *conn,
                              int32_t off, int32_t payload_len)
{
    struct hrpc_conn_reader *reader = &conn->reader;
    struct hrpc_call *call = conn->call;
    int64_t payload_end;
    struct hrpc_response resp;

    // Check if the server sent us a bogus payload_len value.
    if (payload_len < 0) {
        return hadoop_lerr_alloc(EIO, "conn_deliver_resp: "
            "server's payload_len was %" PRId32 ", but negative payload "
            "lengths are not valid.", payload_len);
    }
    payload_end = off;
    payload_end += payload_len;
    if (payload_end > reader->body_len) {
        return hadoop_lerr_alloc(EIO, "conn_deliver_resp: "
            "server's payload_len was %" PRId64 ", but there are only %d "
            "bytes left in the body buffer.", payload_end, reader->body_len);
    }
    // Reset the connection's read state.  We'll hold on to the response buffer
    // while making the callback.
    resp.pb_base = (uint8_t*)(reader->body + off);
    resp.pb_len = payload_len;
    resp.base = reader->body;
    reader->body = NULL;
    free_read_bufs(conn);
    conn->call = NULL;
    conn->reader.state = HRPC_CONN_UNREADABLE;
    // TODO: cache connections 
    hrpc_conn_destroy(conn, NULL);
    hrpc_call_deliver_resp(call, &resp);
    return NULL;
}

static struct hadoop_err *conn_process_response(struct hrpc_conn *conn)
{
    struct hrpc_conn_reader *reader = &conn->reader;
    RpcResponseHeaderProto *resp_header = NULL;
    int32_t off = 0, resp_header_len, payload_len, rem;
    struct hadoop_err *err = NULL;

    if (varint32_decode(&resp_header_len, 
                    (uint8_t*)reader->body, reader->body_len, &off)) {
        err = hadoop_lerr_alloc(EIO, "conn_process_response: response was "
                "only %d bytes-- too short to read the rpc request header.",
                reader->body_len);
        goto done;
    }
    if (resp_header_len <= 0) {
        err = hadoop_lerr_alloc(EIO, "conn_process_response: server sent "
                "invalid resp_header_len of %" PRId32, resp_header_len);
        goto done;
    }
    rem = reader->body_len - off;
    if (resp_header_len > rem) {
        err = hadoop_lerr_alloc(EIO, "conn_process_response: server sent "
                "resp_header_len of %" PRId32 ", but there were only %" PRId32
                " bytes left in the response.", resp_header_len, rem);
        goto done;
    }
    resp_header = rpc_response_header_proto__unpack(NULL, resp_header_len,
                                        (const uint8_t*)(reader->body + off));
    if (!resp_header) {
        err = hadoop_lerr_alloc(EIO, "conn_process_response: failed to "
                "parse RpcRequestHeaderProto.");
        goto done;
    }
    off += resp_header_len;
    if (resp_header->callid != conn->call_id) {
        // We currently only send one request at a time.  So when we get a
        // response, we expect it to be for the request we just sent.
        err = hadoop_lerr_alloc(EIO, "conn_process_response: incorrect call "
                "id in response.  Expected %" PRId32 ", got %" PRId32 ".",
                conn->call_id, resp_header->callid);
        goto done;
    }
    if (resp_header->status != RPC_STATUS_PROTO__SUCCESS) {
        // TODO: keep connection open if we got an ERROR rather than a FATAL.
        err = hadoop_lerr_alloc(EIO, "conn_process_response: error %s: %s",
                  resp_header->exceptionclassname, resp_header->errormsg);
        goto done;
    }
    if (varint32_decode(&payload_len, 
                    (uint8_t*)reader->body, reader->body_len, &off)) {
        err = hadoop_lerr_alloc(EIO, "conn_process_response: header was %d "
                "bytes, and total length was %d-- too short to read the "
                "payload.", resp_header_len, reader->body_len);
        goto done;
    }
    err = conn_deliver_resp(conn, off, payload_len);
done:
    if (resp_header) {
        rpc_response_header_proto__free_unpacked(resp_header, NULL);
    }
    return err;
}

static const char *conn_read_state_str(enum hrpc_conn_read_state state)
{
    switch (state) {
    case HRPC_CONN_UNREADABLE:
        return "HRPC_CONN_UNREADABLE";
    case HRPC_CONN_READ_LEN:
        return "HRPC_CONN_READ_LEN";
    case HRPC_CONN_READ_BODY:
        return "HRPC_CONN_READ_BODY";
    case HRPC_CONN_READ_CLOSED:
        return "HRPC_CONN_READ_CLOSED";
    default:
        return "(unknown)";
    }
};

/**
 * Return a read buffer to libuv.
 *
 * We don't do the actual allocation here, for two reasons.  The first is that
 * we'd like to know how big a buffer to allocate first (by reading the first
 * 4 bytes).  The second is that libuv doesn't really take kindly to
 * failures here... returning a zero-length buffer triggers a crash.
 * So we simply return previously allocated buffers here.
 *
 * @param handle            The TCP stream.
 * @param suggested_size    The suggested size.
 *
 * @return                  The read buffer to use.
 */
static void hrpc_conn_read_alloc(uv_handle_t *handle,
        size_t suggested_size __attribute__((unused)), uv_buf_t *buf)
{
    int32_t rem;
    struct hrpc_conn *conn = handle->data;
    struct hrpc_conn_reader *reader = &conn->reader;

    switch (reader->state) {
    case HRPC_CONN_READ_LEN:
        buf->base = (char*)(reader->body_len_buf + reader->off);
        buf->len = READLEN_BUF_LEN - reader->off;
        return;
    case HRPC_CONN_READ_BODY:
        rem = reader->body_len - reader->off;
        if (rem <= 0) {
            conn_log_warn(conn, "hrpc_conn_read_alloc: we're in state "
                    "HRPC_CONN_READ_BODY with reader->body_len = %" PRId32 ", but "
                    "reader->off = %" PRId32 "\n", reader->body_len, reader->off);
            buf->base = NULL;
            buf->len = 0;
            return;
        }
        buf->base = (char*)(reader->body + reader->off);
        buf->len = rem;
        return;
    default:
        conn_log_warn(conn, "hrpc_conn_read_alloc: we're in state "
                "%s, but we're being asked to allocate "
                "a read buffer.\n", conn_read_state_str(reader->state));
        buf->base = NULL;
        buf->len = 0;
        return;
    }
}

/**
 * The read callback for this connection.
 */
static void conn_read_cb(uv_stream_t *stream, ssize_t nread,
                         const uv_buf_t* buf __attribute__((unused)))
{
    struct hrpc_conn *conn = stream->data;
    struct hrpc_conn_reader *reader = &conn->reader;
    struct hadoop_err *err;

    if (nread < 0) {
        hrpc_conn_destroy(conn,
            hadoop_uverr_alloc(-nread, "conn_read_cb error"));
        return;
    }
    if (nread == 0) {
        // Nothing to do.
        return;
    }
    switch (reader->state) {
    case HRPC_CONN_READ_LEN:
        reader->off += nread;
        if (reader->off < READLEN_BUF_LEN) {
            conn_log_debug(conn, "conn_read_cb: got partial read of "
                           "body_len.  reader->off = %" PRId32 "\n",
                           reader->off);
            return;
        }
        reader->body_len = be32_decode(reader->body_len_buf);
        if ((reader->body_len <= 0) ||
                (reader->body_len > MAX_READER_BODY_LEN)) {
            hrpc_conn_destroy(conn, hadoop_lerr_alloc(EIO, 
                "conn_read_cb: got an invalid body length of %" PRId32 "\n", 
                reader->body_len));
            return;
        }
        conn_log_debug(conn, "conn_read_cb: got body length of "
                       "%" PRId32 ".  Transitioning to HRPC_CONN_READ_BODY.\n",
                       reader->body_len);
        reader->off = 0;
        reader->state = HRPC_CONN_READ_BODY;
        reader->body = malloc(reader->body_len);
        if (!reader->body) {
            hrpc_conn_destroy(conn, hadoop_lerr_alloc(ENOMEM,
                    "hrpc_conn_read_alloc: failed to allocate "
                    "%" PRId32 " bytes.\n", reader->body_len));
        }
        break;
    case HRPC_CONN_READ_BODY:
        reader->off += nread;
        if (reader->off < reader->body_len) {
            conn_log_debug(conn, "conn_read_cb: got partial read of "
                           "body.  reader->off = %" PRId32 " out of %"
                           PRId32 "\n", reader->off, reader->body_len);
            return;
        }
        err = conn_process_response(conn);
        free_read_bufs(conn);
        if (err) {
            hrpc_conn_destroy(conn, err);
            return;
        }
        reader->state = HRPC_CONN_UNREADABLE;
        break;

    default:
        conn_log_warn(conn, "conn_read_cb: got an unexpected read "
                      "event while in %s state.\n",
                      conn_read_state_str(reader->state));
        return;
    }
}

static void conn_free(uv_handle_t* handle)
{
    struct hrpc_conn *conn = handle->data;
    free(conn);
}

void hrpc_conn_destroy(struct hrpc_conn *conn, struct hadoop_err *err)
{
    reactor_remove_conn(conn->reactor, conn);
    if (conn->call) {
        err = err ? err : hadoop_lerr_alloc(EFAULT, "hrpc_conn_destroy: "
                "internal error: shutting down connection while it "
                "still has a call in progress.");
        conn_log_warn(conn, "hrpc_conn_destroy: %s\n", hadoop_err_msg(err));
        hrpc_call_deliver_err(conn->call, err);
        conn->call = NULL;
    } else if (err) {
        conn_log_warn(conn, "hrpc_conn_destroy: %s\n", hadoop_err_msg(err));
        hadoop_err_free(err);
    }
    free_read_bufs(conn);
    conn->reader.state = HRPC_CONN_READ_CLOSED;
    free_write_bufs(conn);
    conn->writer.state = HRPC_CONN_WRITE_CLOSED;
    free(conn->protocol);
    free(conn->username);
    uv_close((uv_handle_t*)&conn->stream, conn_free);
}

// vim: ts=4:sw=4:tw=79:et
