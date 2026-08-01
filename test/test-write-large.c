/* Copyright libuv project contributors. All rights reserved.
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

/* Tests for stream writes that are (much) larger than what the operating
 * system accepts in a single kernel submission. libuv splits such writes into
 * bounded submissions internally; a single uv_write() of arbitrary size must
 * behave as one request with one callback, and uv_cancel() must be able to
 * stop it between submissions with an accurate uv_write_nwritten(). */

#include <stdlib.h>
#include <string.h>

#include "uv.h"
#include "task.h"

#define MB ((size_t) 1 << 20)

/* Large enough to exceed both the ~2 GB unix per-syscall limit (0x7ffff000
 * bytes) and the Windows internal chunk size. */
#define LARGE_WRITE_SIZE (((size_t) 1 << 31) + 16 * MB)

/* Exceeds the Windows internal chunk size (511 MB) so the write is submitted
 * in multiple chunks there, while staying cheap enough to run everywhere. */
#define ORDERING_WRITE_SIZE ((size_t) 0x28000000) /* 640 MB */
#define ORDERING_SMALL_SIZE (64 * 1024)
#define ORDERING_SMALL_COUNT 3

#define CANCEL_WRITE_SIZE ((size_t) 4 * 1024 * MB) /* 4 GB */
#define CANCEL_AFTER_BYTES ((size_t) 256 * MB)

static uv_pipe_t reader;
static uv_pipe_t writer;
static uv_write_t write_req;
static uv_write_t small_reqs[ORDERING_SMALL_COUNT];

static char* write_data;
static size_t write_size;

static size_t bytes_received;
static int write_cb_called;
static int small_write_cbs_called;
static int close_cb_called;
static int cancel_issued;
static size_t received_at_cancel;
static size_t expected_stream_size;

static char read_buf[1 * MB];


/* The byte expected at absolute stream offset `offset`: constant within each
 * MB, pseudo-random across MBs, so lost, duplicated or reordered stretches
 * are detected no matter where they happen. */
static char pattern_byte(size_t offset) {
  return (char) (((offset >> 20) * 2654435761u) >> 24);
}


static void fill_pattern(char* p, size_t base, size_t len) {
  size_t offset;
  size_t n;

  for (offset = 0; offset < len; offset += n) {
    n = MB - ((base + offset) & (MB - 1));
    if (n > len - offset)
      n = len - offset;
    memset(p + offset, pattern_byte(base + offset), n);
  }
}


static void close_cb(uv_handle_t* handle) {
  close_cb_called++;
}


static void alloc_cb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  buf->base = read_buf;
  buf->len = sizeof(read_buf);
}


static void read_verify_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  ssize_t i;

  if (nread == UV_EOF) {
    ASSERT_UINT64_EQ(expected_stream_size, bytes_received);
    uv_close((uv_handle_t*) &reader, close_cb);
    return;
  }

  ASSERT_GE(nread, 0);

  for (i = 0; i < nread; i++)
    ASSERT_EQ(buf->base[i], pattern_byte(bytes_received + i));

  bytes_received += nread;
}


static void large_write_cb(uv_write_t* req, int status) {
  ASSERT_PTR_EQ(req, &write_req);
  ASSERT_OK(status);
  ASSERT_UINT64_EQ(write_size, uv_write_nwritten(req));
  write_cb_called++;

  /* Nothing left to write: signal EOF to the reader. */
  uv_close((uv_handle_t*) &writer, close_cb);
}


static int large_test_prologue(size_t total, uint64_t required_memory) {
  uv_os_fd_t fds[2];

  if (sizeof(size_t) < 8)
    return 1; /* Not addressable on 32 bits. */

  if (uv_get_total_memory() < required_memory)
    return 1;

  write_data = malloc(total);
  if (write_data == NULL)
    return 1;

  write_size = total;

  ASSERT_OK(uv_pipe(fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE));

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &reader, 0));
  ASSERT_OK(uv_pipe_open(&reader, fds[0]));
  ASSERT_OK(uv_pipe_init(uv_default_loop(), &writer, 0));
  ASSERT_OK(uv_pipe_open(&writer, fds[1]));

  ASSERT_OK(uv_read_start((uv_stream_t*) &reader, alloc_cb, read_verify_cb));

  return 0;
}


/* One uv_write() larger than the OS accepts per submission: must be delivered
 * completely, with a single callback reporting success. */
TEST_IMPL(write_large) {
  uv_buf_t buf;

  if (large_test_prologue(LARGE_WRITE_SIZE, 6 * (uint64_t) 1024 * 1024 * 1024))
    RETURN_SKIP("not enough memory or address space for large write test");

  fill_pattern(write_data, 0, write_size);
  expected_stream_size = write_size;

  buf = uv_buf_init(write_data, (unsigned int) write_size);
  ASSERT_OK(uv_write(&write_req, (uv_stream_t*) &writer, &buf, 1,
                     large_write_cb));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));

  ASSERT_EQ(1, write_cb_called);
  ASSERT_EQ(2, close_cb_called);
  ASSERT_UINT64_EQ(write_size, bytes_received);

  free(write_data);
  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


static void ordering_small_write_cb(uv_write_t* req, int status) {
  ASSERT_OK(status);
  /* The large write's callback must have fired before any of the writes
   * queued up behind it. */
  ASSERT_EQ(1, write_cb_called);
  ASSERT_PTR_EQ(req, &small_reqs[small_write_cbs_called]);
  ASSERT_UINT64_EQ(ORDERING_SMALL_SIZE, uv_write_nwritten(req));
  small_write_cbs_called++;

  if (small_write_cbs_called == ORDERING_SMALL_COUNT)
    uv_close((uv_handle_t*) &writer, close_cb);
}


static void ordering_large_write_cb(uv_write_t* req, int status) {
  ASSERT_PTR_EQ(req, &write_req);
  ASSERT_OK(status);
  ASSERT_UINT64_EQ(write_size, uv_write_nwritten(req));
  ASSERT_OK(small_write_cbs_called);
  write_cb_called++;
}


/* Writes queued while a larger-than-a-single-submission write is in flight
 * must neither interleave with it on the wire nor overtake it. */
TEST_IMPL(write_large_ordering) {
  char* small_data;
  uv_buf_t buf;
  size_t offset;
  int i;

  if (large_test_prologue(ORDERING_WRITE_SIZE,
                          2 * (uint64_t) 1024 * 1024 * 1024))
    RETURN_SKIP("not enough memory or address space for large write test");

  fill_pattern(write_data, 0, write_size);

  small_data = malloc(ORDERING_SMALL_COUNT * ORDERING_SMALL_SIZE);
  ASSERT_NOT_NULL(small_data);

  buf = uv_buf_init(write_data, (unsigned int) write_size);
  ASSERT_OK(uv_write(&write_req, (uv_stream_t*) &writer, &buf, 1,
                     ordering_large_write_cb));

  /* Issue trailing writes right away; their data continues the pattern of
   * the large write, so any interleaving or reordering trips the reader. */
  offset = write_size;
  for (i = 0; i < ORDERING_SMALL_COUNT; i++) {
    char* p = small_data + i * ORDERING_SMALL_SIZE;
    fill_pattern(p, offset, ORDERING_SMALL_SIZE);
    buf = uv_buf_init(p, ORDERING_SMALL_SIZE);
    ASSERT_OK(uv_write(&small_reqs[i], (uv_stream_t*) &writer, &buf, 1,
                       ordering_small_write_cb));
    offset += ORDERING_SMALL_SIZE;
  }

  expected_stream_size = offset;

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));

  ASSERT_EQ(1, write_cb_called);
  ASSERT_EQ(ORDERING_SMALL_COUNT, small_write_cbs_called);
  ASSERT_EQ(2, close_cb_called);
  ASSERT_UINT64_EQ(expected_stream_size, bytes_received);

  free(write_data);
  free(small_data);
  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


static void cancel_write_cb(uv_write_t* req, int status) {
  ASSERT_PTR_EQ(req, &write_req);
  ASSERT_EQ(UV_ECANCELED, status);

  /* Everything already handed to the kernel when the cancellation landed is
   * counted; nothing more will be. The reader has seen at most that much. */
  ASSERT_UINT64_GE(uv_write_nwritten(req), received_at_cancel);
  ASSERT_UINT64_LT(uv_write_nwritten(req), write_size);
  write_cb_called++;

  /* The bytes that were written must be exactly the bytes that arrive. */
  expected_stream_size = uv_write_nwritten(req);
  uv_close((uv_handle_t*) &writer, close_cb);
}


static void cancel_read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  if (nread == UV_EOF) {
    ASSERT_EQ(1, write_cb_called);
    ASSERT_UINT64_EQ(expected_stream_size, bytes_received);
    uv_close((uv_handle_t*) &reader, close_cb);
    return;
  }

  ASSERT_GE(nread, 0);
  bytes_received += nread;

  if (bytes_received >= CANCEL_AFTER_BYTES && !cancel_issued) {
    cancel_issued = 1;
    received_at_cancel = bytes_received;
    ASSERT_OK(uv_cancel((uv_req_t*) &write_req));
  }
}


/* uv_cancel() on a huge in-flight write: the single callback reports
 * UV_ECANCELED and uv_write_nwritten() the exact number of bytes written
 * before the cancellation, all of which (and nothing more) are delivered. */
TEST_IMPL(write_large_cancel) {
  uv_buf_t bufs[2];

  if (large_test_prologue(CANCEL_WRITE_SIZE, 6 * (uint64_t) 1024 * 1024 * 1024))
    RETURN_SKIP("not enough memory or address space for large write test");

  /* Content is not verified (the write is cancelled at an arbitrary point);
   * only delivery accounting is. The buffer is left untouched so the pages
   * do not need to be materialized up front. */

  ASSERT_OK(uv_read_stop((uv_stream_t*) &reader));
  ASSERT_OK(uv_read_start((uv_stream_t*) &reader, alloc_cb, cancel_read_cb));

  /* Two buffers, because a single uv_buf_t cannot describe 4 GB on Windows;
   * this also exercises the multi-buffer bounded-submission path. */
  bufs[0] = uv_buf_init(write_data, (unsigned int) (write_size / 2));
  bufs[1] = uv_buf_init(write_data + write_size / 2,
                        (unsigned int) (write_size / 2));
  ASSERT_OK(uv_write(&write_req, (uv_stream_t*) &writer, bufs, 2,
                     cancel_write_cb));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));

  ASSERT_EQ(1, write_cb_called);
  ASSERT_EQ(1, cancel_issued);
  ASSERT_EQ(2, close_cb_called);

  free(write_data);
  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}
