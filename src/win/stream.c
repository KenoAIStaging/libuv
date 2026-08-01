/* Copyright Joyent, Inc. and other Node contributors. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <assert.h>

#include "uv.h"
#include "internal.h"
#include "handle-inl.h"
#include "req-inl.h"


int uv_listen(uv_stream_t* stream, int backlog, uv_connection_cb cb) {
  int err;
  if (uv__is_closing(stream)) {
    return UV_EINVAL;
  }
  err = ERROR_INVALID_PARAMETER;
  switch (stream->type) {
    case UV_TCP:
      err = uv__tcp_listen((uv_tcp_t*)stream, backlog, cb);
      break;
    case UV_NAMED_PIPE:
      err = uv__pipe_listen((uv_pipe_t*)stream, backlog, cb);
      break;
    default:
      assert(0);
  }

  return uv_translate_sys_error(err);
}


int uv_accept(uv_stream_t* server, uv_stream_t* client) {
  int err;

  err = ERROR_INVALID_PARAMETER;
  switch (server->type) {
    case UV_TCP:
      err = uv__tcp_accept((uv_tcp_t*)server, (uv_tcp_t*)client);
      break;
    case UV_NAMED_PIPE:
      err = uv__pipe_accept((uv_pipe_t*)server, client);
      break;
    default:
      assert(0);
  }

  return uv_translate_sys_error(err);
}


int uv__read_start(uv_stream_t* handle,
                   uv_alloc_cb alloc_cb,
                   uv_read_cb read_cb) {
  int err;

  err = ERROR_INVALID_PARAMETER;
  switch (handle->type) {
    case UV_TCP:
      err = uv__tcp_read_start((uv_tcp_t*)handle, alloc_cb, read_cb);
      break;
    case UV_NAMED_PIPE:
      err = uv__pipe_read_start((uv_pipe_t*)handle, alloc_cb, read_cb);
      break;
    case UV_TTY:
      err = uv__tty_read_start((uv_tty_t*) handle, alloc_cb, read_cb);
      break;
    default:
      assert(0);
  }

  return uv_translate_sys_error(err);
}


int uv_read_stop(uv_stream_t* handle) {
  int err;

  if (!(handle->flags & UV_HANDLE_READING))
    return 0;

  err = 0;
  if (handle->type == UV_TTY) {
    err = uv__tty_read_stop((uv_tty_t*) handle);
  } else if (handle->type == UV_NAMED_PIPE) {
    uv__pipe_read_stop((uv_pipe_t*) handle);
  } else {
    handle->flags &= ~UV_HANDLE_READING;
    DECREASE_ACTIVE_COUNT(handle->loop, handle);
  }

  return uv_translate_sys_error(err);
}


int uv_write(uv_write_t* req,
             uv_stream_t* handle,
             const uv_buf_t bufs[],
             unsigned int nbufs,
             uv_write_cb cb) {
  uv_loop_t* loop = handle->loop;
  int err;

  if (!(handle->flags & UV_HANDLE_WRITABLE)) {
    return UV_EPIPE;
  }

  err = ERROR_INVALID_PARAMETER;
  switch (handle->type) {
    case UV_TCP:
      err = uv__tcp_write(loop, req, (uv_tcp_t*) handle, bufs, nbufs, cb);
      break;
    case UV_NAMED_PIPE:
      err = uv__pipe_write(
          loop, req, (uv_pipe_t*) handle, bufs, nbufs, NULL, cb);
      break;
    case UV_TTY:
      err = uv__tty_write(loop, req, (uv_tty_t*) handle, bufs, nbufs, cb);
      break;
    default:
      assert(0);
  }

  return uv_translate_sys_error(err);
}


int uv_write2(uv_write_t* req,
              uv_stream_t* handle,
              const uv_buf_t bufs[],
              unsigned int nbufs,
              uv_stream_t* send_handle,
              uv_write_cb cb) {
  uv_loop_t* loop = handle->loop;
  int err;

  if (send_handle == NULL) {
    return uv_write(req, handle, bufs, nbufs, cb);
  }

  if (handle->type != UV_NAMED_PIPE || !((uv_pipe_t*) handle)->ipc) {
    return UV_EINVAL;
  } else if (!(handle->flags & UV_HANDLE_WRITABLE)) {
    return UV_EPIPE;
  }

  err = uv__pipe_write(
      loop, req, (uv_pipe_t*) handle, bufs, nbufs, send_handle, cb);
  return uv_translate_sys_error(err);
}


int uv_try_write(uv_stream_t* stream,
                 const uv_buf_t bufs[],
                 unsigned int nbufs) {
  if (stream->flags & UV_HANDLE_CLOSING)
    return UV_EBADF;
  if (!(stream->flags & UV_HANDLE_WRITABLE))
    return UV_EPIPE;

  switch (stream->type) {
    case UV_TCP:
      return uv__tcp_try_write((uv_tcp_t*) stream, bufs, nbufs);
    case UV_TTY:
      return uv__tty_try_write((uv_tty_t*) stream, bufs, nbufs);
    case UV_NAMED_PIPE:
      return UV_EAGAIN;
    default:
      assert(0);
      return UV_ENOSYS;
  }
}


int uv_try_write2(uv_stream_t* stream,
                  const uv_buf_t bufs[],
                  unsigned int nbufs,
                  uv_stream_t* send_handle) {
  if (send_handle != NULL)
    return UV_EAGAIN;
  return uv_try_write(stream, bufs, nbufs);
}


int uv_shutdown(uv_shutdown_t* req, uv_stream_t* handle, uv_shutdown_cb cb) {
  uv_loop_t* loop = handle->loop;

  if (!(handle->flags & UV_HANDLE_WRITABLE) ||
      uv__is_stream_shutting(handle) ||
      uv__is_closing(handle)) {
    return UV_ENOTCONN;
  }

  UV_REQ_INIT(loop, req, UV_SHUTDOWN);
  req->handle = handle;
  req->cb = cb;

  handle->flags &= ~UV_HANDLE_WRITABLE;
  handle->stream.conn.shutdown_req = req;
  handle->reqs_pending++;
  REGISTER_HANDLE_REQ(loop, handle, req);

  if (handle->stream.conn.write_reqs_pending == 0) {
    if (handle->type == UV_NAMED_PIPE)
      uv__pipe_shutdown(loop, (uv_pipe_t*) handle, req);
    else
      uv__insert_pending_req(loop, (uv_req_t*) req);
  }

  return 0;
}


int uv_is_readable(const uv_stream_t* handle) {
  return !!(handle->flags & UV_HANDLE_READABLE);
}


int uv_is_writable(const uv_stream_t* handle) {
  return !!(handle->flags & UV_HANDLE_WRITABLE);
}


int uv_stream_set_blocking(uv_stream_t* handle, int blocking) {
  if (handle->type != UV_NAMED_PIPE)
    return UV_EINVAL;

  if (blocking != 0)
    handle->flags |= UV_HANDLE_BLOCKING_WRITES;
  else
    handle->flags &= ~UV_HANDLE_BLOCKING_WRITES;

  return 0;
}


size_t uv_write_nwritten(const uv_write_t* req) {
  return req->write_extra.nwritten;
}


/* Account for `n` more bytes of a chunked write having been accepted by the
 * kernel, advancing the request's buffer cursor. This mirrors
 * uv__write_req_update in the Unix implementation. Returns 1 when the
 * request's buffers are exhausted, 0 when there is more data to submit. */
int uv__write_req_chunk_update(uv_stream_t* handle,
                               uv_write_t* req,
                               size_t n) {
  uv_buf_t* buf;
  size_t len;

  assert(req->bufs != NULL);
  assert(req->write_index < req->nbufs);
  assert(handle->write_queue_size >= n);
  assert(req->u.io.queued_bytes >= n);

  handle->write_queue_size -= n;
  req->u.io.queued_bytes -= n;
  req->write_extra.nwritten += n;

  buf = req->bufs + req->write_index;

  do {
    len = n < buf->len ? n : buf->len;
    buf->base += len;
    buf->len -= (ULONG) len;
    buf += (buf->len == 0);  /* Advance to next buffer if this one is empty. */
    n -= len;
  } while (n > 0);

  req->write_index = (unsigned int) (buf - req->bufs);

  return req->write_index == req->nbufs;
}


void uv__write_req_chunk_cleanup(uv_write_t* req) {
  if (req->bufs != NULL) {
    if (req->bufs != req->bufsml)
      uv__free(req->bufs);
    req->bufs = NULL;
  }
}


/* Writes deferred behind an in-progress chunked write are kept on a circular
 * singly-linked list (through next_req) whose entry point is the list's tail,
 * so both head removal and tail insertion are O(1). This matches the layout
 * of the non-overlapped pipe write queue. */
void uv__stream_defer_write(uv_stream_t* handle, uv_write_t* req) {
  uv_write_t* tail;

  assert(handle->stream.conn.chunked_write != NULL);

  tail = handle->stream.conn.deferred_writes_tail;
  if (tail != NULL) {
    req->next_req = tail->next_req;
    tail->next_req = (uv_req_t*) req;
  } else {
    req->next_req = (uv_req_t*) req;
  }
  handle->stream.conn.deferred_writes_tail = req;
}


/* Remove the element after prev from the deferred write queue. prev must be
 * a node in the queue. Returns the removed element. */
static uv_write_t* uv__stream_deferred_write_remove_after(uv_stream_t* handle,
                                                          uv_write_t* prev) {
  uv_write_t* req;

  req = (uv_write_t*) prev->next_req;
  if (req == prev) {
    /* Only element. */
    handle->stream.conn.deferred_writes_tail = NULL;
  } else {
    prev->next_req = req->next_req;
    if (req == handle->stream.conn.deferred_writes_tail)
      handle->stream.conn.deferred_writes_tail = prev;
  }
  return req;
}


uv_write_t* uv__stream_deferred_write_dequeue(uv_stream_t* handle) {
  if (handle->stream.conn.deferred_writes_tail == NULL)
    return NULL;

  return uv__stream_deferred_write_remove_after(
      handle, handle->stream.conn.deferred_writes_tail);
}


/* Find and remove a specific request from the deferred write queue. For
 * coalesced writes, match against the user-facing req. Returns the actual
 * queued req if found and removed, NULL otherwise. */
uv_write_t* uv__stream_deferred_write_remove(uv_stream_t* handle,
                                             uv_write_t* target) {
  uv_write_t* tail;
  uv_write_t* prev;
  uv_write_t* curr;

  tail = handle->stream.conn.deferred_writes_tail;
  if (tail == NULL)
    return NULL;

  prev = tail;
  curr = (uv_write_t*) tail->next_req;
  do {
    if (uv__write_user_req(curr) == target)
      return uv__stream_deferred_write_remove_after(handle, prev);

    prev = curr;
    curr = (uv_write_t*) curr->next_req;
  } while (prev != tail);

  return NULL;
}


/* Complete all deferred writes as cancelled. Used when the handle is being
 * closed; the requests were never submitted to the kernel, so they can be
 * failed directly through the completion path. */
void uv__stream_flush_deferred_writes(uv_stream_t* handle) {
  uv_write_t* req;

  while ((req = uv__stream_deferred_write_dequeue(handle)) != NULL) {
    SET_REQ_ERROR(req, ERROR_OPERATION_ABORTED);
    SET_REQ_NWRITTEN(req, 0);
    uv__insert_pending_req(handle->loop, (uv_req_t*) req);
  }
}


int uv__write_cancel(uv_write_t* req) {
  uv_stream_t* stream;
  uv_write_t* queued;
  uv_write_t* chunked;
  HANDLE handle;
  BOOL result;

  stream = req->handle;

  switch (stream->type) {
    case UV_TCP:
      handle = (HANDLE) ((uv_tcp_t*) stream)->socket;
      break;
    case UV_NAMED_PIPE:
      handle = ((uv_pipe_t*) stream)->handle;

      if ((stream->flags & (UV_HANDLE_BLOCKING_WRITES | UV_HANDLE_NON_OVERLAPPED_PIPE)) ==
          UV_HANDLE_NON_OVERLAPPED_PIPE) {
        return uv__pipe_write_cancel_non_overlapped((uv_pipe_t*) stream, req);
      }

      break;
    case UV_TTY:
      /* TTY writes complete synchronously on Windows, so cancellation
       * is not applicable - the callback has already been queued. */
      return 0;
    default:
      return UV_EINVAL;
  }

  /* Writes deferred behind an in-progress chunked write have no kernel
   * operation outstanding; complete them as cancelled directly. */
  queued = uv__stream_deferred_write_remove(stream, req);
  if (queued != NULL) {
    SET_REQ_ERROR(queued, ERROR_OPERATION_ABORTED);
    SET_REQ_NWRITTEN(queued, 0);
    uv__insert_pending_req(stream->loop, (uv_req_t*) queued);
    return 0;
  }

  chunked = stream->stream.conn.chunked_write;
  if (chunked != NULL && uv__write_user_req(chunked) == req) {
    /* Stop the write at the next chunk boundary, and also try to abort the
     * chunk that is currently in flight. Even if that chunk completes
     * successfully before the cancellation lands, the request completes
     * with UV_ECANCELED - and an accurate uv_write_nwritten() - unless it
     * was the final chunk, in which case the whole write succeeded. */
    chunked->cancel_requested = 1;
    req = chunked; /* CancelIoEx must target the overlapped in actual use. */
  }

  result = CancelIoEx(handle, &req->u.io.overlapped);

  if (!result) {
    DWORD err = GetLastError();
    if (err == ERROR_NOT_FOUND) {
      /* The operation has already completed. */
      return 0;
    }
    return uv_translate_sys_error(err);
  }

  return 0;
}
