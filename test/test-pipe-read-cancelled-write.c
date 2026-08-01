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
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/* Regression test: a libuv pipe reader must survive a peer cancelling its
 * pended (in-flight) large pipe write.
 *
 * The named pipe file system services reads for a peer's pended write
 * directly from the peer's buffer and advertises those bytes to
 * PeekNamedPipe; when the peer cancels the write with CancelIoEx, the
 * advertised-but-unread bytes are retracted. The reader's event loop used to
 * wedge forever in a synchronous ReadFile sized from the stale peek. This
 * test parks the reader's loop mid-drain (inside read_cb) while a raw Win32
 * writer thread cancels its own pended overlapped WriteFile, and then
 * asserts that the loop stays live (a repeating timer keeps firing), that
 * data written after the cancellation is still delivered intact, and that
 * closing the write end yields a clean EOF.
 */

#include "uv.h"
#include "task.h"

#include <stdio.h>
#include <string.h>

#ifndef _WIN32

TEST_IMPL(pipe_read_cancelled_write) {
  RETURN_SKIP("Test requires a raw Win32 overlapped named pipe writer.");
}

#else /* _WIN32 */

#define BIG_WRITE_SIZE (64 * 1024 * 1024)
#define READ_PARK_THRESHOLD (8 * 1024 * 1024)
#define RECOVERY_PAYLOAD "PIPE-RECOVERED!!"
#define RECOVERY_SIZE (sizeof(RECOVERY_PAYLOAD) - 1)
#define POST_CANCEL_TICKS 5

static uv_pipe_t reader;
static uv_timer_t liveness_timer;
static uv_thread_t writer_thread;

static HANDLE write_end;
static HANDLE pended_event;    /* writer -> main: big write is in flight */
static HANDLE reach_event;     /* read_cb -> writer: threshold consumed */
static HANDLE cancelled_event; /* writer -> read_cb: write cancelled */
static HANDLE resume_event;    /* timer_cb -> writer: loop proven live */

static char read_slab[1024 * 1024];
static char tail[RECOVERY_SIZE];

static uint64_t bytes_received;
static DWORD writer_reported;   /* transfer count of the cancelled write */
static int write_was_pending;
static int reach_signaled;
static int park_over;
static int post_cancel_ticks;
static int loop_proven_live;
static int eof_seen;
static int close_cb_called;


static void writer_thread_proc(void* arg) {
  char* big;
  OVERLAPPED ov;
  DWORD transferred;
  DWORD err;
  BOOL r;

  big = (char*) malloc(BIG_WRITE_SIZE);
  ASSERT_NOT_NULL(big);
  memset(big, 0xAB, BIG_WRITE_SIZE);

  memset(&ov, 0, sizeof(ov));
  ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(ov.hEvent);

  /* Pend one large overlapped write. It is much larger than the pipe's
   * quota and nobody is reading yet, so it cannot complete synchronously. */
  r = WriteFile(write_end, big, BIG_WRITE_SIZE, NULL, &ov);
  ASSERT_EQ(0, r);
  ASSERT_EQ((DWORD) ERROR_IO_PENDING, GetLastError());
  write_was_pending = 1;
  ASSERT_NE(0, SetEvent(pended_event));

  /* Wait until the reader has consumed a bounded prefix, then cancel the
   * rest of the write out from under it. */
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(reach_event, 30000));
  CancelIoEx(write_end, &ov);
  transferred = 0;
  if (!GetOverlappedResult(write_end, &ov, &transferred, TRUE)) {
    err = GetLastError();
    ASSERT_EQ((DWORD) ERROR_OPERATION_ABORTED, err);
  }
  writer_reported = transferred;
  ASSERT_NE(0, SetEvent(cancelled_event));

  /* Wait for the reader's loop to prove it is still alive, then exercise
   * the pipe again: post-cancellation writes must still be delivered. */
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(resume_event, 30000));
  CloseHandle(ov.hEvent);
  memset(&ov, 0, sizeof(ov));
  ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(ov.hEvent);
  if (!WriteFile(write_end, RECOVERY_PAYLOAD, (DWORD) RECOVERY_SIZE, NULL, &ov))
    ASSERT_EQ((DWORD) ERROR_IO_PENDING, GetLastError());
  ASSERT_NE(0, GetOverlappedResult(write_end, &ov, &transferred, TRUE));
  ASSERT_EQ((DWORD) RECOVERY_SIZE, transferred);
  CloseHandle(ov.hEvent);

  /* EOF for the reader. */
  CloseHandle(write_end);
  write_end = INVALID_HANDLE_VALUE;
  free(big);
}


static void close_cb(uv_handle_t* handle) {
  close_cb_called++;
}


static void alloc_cb(uv_handle_t* handle,
                     size_t suggested_size,
                     uv_buf_t* buf) {
  buf->base = read_slab;
  buf->len = sizeof(read_slab);
}


static void read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  size_t keep;
  size_t i;

  if (nread == UV_EOF) {
    eof_seen = 1;
    uv_close((uv_handle_t*) &reader, close_cb);
    uv_close((uv_handle_t*) &liveness_timer, close_cb);
    return;
  }

  ASSERT_GE(nread, 0);

  /* Track the last RECOVERY_SIZE bytes of the stream. */
  if ((size_t) nread >= RECOVERY_SIZE) {
    memcpy(tail, buf->base + nread - RECOVERY_SIZE, RECOVERY_SIZE);
  } else if (nread > 0) {
    keep = RECOVERY_SIZE - (size_t) nread;
    memmove(tail, tail + RECOVERY_SIZE - keep, keep);
    for (i = 0; i < (size_t) nread; i++)
      tail[keep + i] = buf->base[i];
  }

  bytes_received += (uint64_t) nread;

  if (bytes_received >= READ_PARK_THRESHOLD && !reach_signaled) {
    reach_signaled = 1;
    /* Park the loop in the middle of the drain loop, exactly where the
     * stale-peek wedge used to occur, while the writer thread cancels its
     * in-flight write. When this returns, the peeked byte count that the
     * reader is still working through is guaranteed stale. */
    ASSERT_NE(0, SetEvent(reach_event));
    ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(cancelled_event, 30000));
    park_over = 1;
  }
}


static void timer_cb(uv_timer_t* timer) {
  if (park_over && !loop_proven_live && ++post_cancel_ticks >= POST_CANCEL_TICKS) {
    /* The loop kept turning after the retraction: the reader did not wedge.
     * Let the writer finish (recovery write + EOF). */
    loop_proven_live = 1;
    ASSERT_NE(0, SetEvent(resume_event));
  }
}


TEST_IMPL(pipe_read_cancelled_write) {
  char name[128];
  HANDLE read_end;
  OVERLAPPED conn_ov;
  DWORD err;

  snprintf(name,
           sizeof(name),
           "\\\\.\\pipe\\uv-read-cancelled-write-%lu",
           (unsigned long) GetCurrentProcessId());

  read_end = CreateNamedPipeA(name,
                              PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED |
                                  FILE_FLAG_FIRST_PIPE_INSTANCE,
                              PIPE_TYPE_BYTE | PIPE_WAIT,
                              1,
                              65536,
                              65536,
                              0,
                              NULL);
  ASSERT_PTR_NE(read_end, INVALID_HANDLE_VALUE);

  write_end = CreateFileA(name,
                          GENERIC_WRITE,
                          0,
                          NULL,
                          OPEN_EXISTING,
                          FILE_FLAG_OVERLAPPED,
                          NULL);
  ASSERT_PTR_NE(write_end, INVALID_HANDLE_VALUE);

  memset(&conn_ov, 0, sizeof(conn_ov));
  conn_ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(conn_ov.hEvent);
  if (!ConnectNamedPipe(read_end, &conn_ov)) {
    err = GetLastError();
    if (err == ERROR_IO_PENDING)
      ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(conn_ov.hEvent, 10000));
    else
      ASSERT_EQ((DWORD) ERROR_PIPE_CONNECTED, err);
  }
  CloseHandle(conn_ov.hEvent);

  pended_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  reach_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  cancelled_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  resume_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(pended_event);
  ASSERT_NOT_NULL(reach_event);
  ASSERT_NOT_NULL(cancelled_event);
  ASSERT_NOT_NULL(resume_event);

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &reader, 0));
  ASSERT_OK(uv_pipe_open(&reader, read_end));

  ASSERT_OK(uv_timer_init(uv_default_loop(), &liveness_timer));
  ASSERT_OK(uv_timer_start(&liveness_timer, timer_cb, 10, 10));

  /* Only start reading once the big write is already in flight, so that the
   * doorbell read and the peek see (and advertise) its bytes. */
  ASSERT_OK(uv_thread_create(&writer_thread, writer_thread_proc, NULL));
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(pended_event, 30000));
  ASSERT_OK(uv_read_start((uv_stream_t*) &reader, alloc_cb, read_cb));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));
  ASSERT_OK(uv_thread_join(&writer_thread));

  ASSERT_EQ(1, write_was_pending);
  ASSERT_EQ(1, reach_signaled);
  ASSERT_EQ(1, park_over);
  ASSERT_EQ(1, loop_proven_live); /* The loop kept firing after retraction. */
  ASSERT_EQ(1, eof_seen);
  ASSERT_EQ(2, close_cb_called);

  /* The cancellation truncated the big write... */
  ASSERT_LT(writer_reported, (DWORD) BIG_WRITE_SIZE);
  /* ...the reader consumed the park threshold plus the recovery payload... */
  ASSERT_UINT64_GE(bytes_received, READ_PARK_THRESHOLD + RECOVERY_SIZE);
  /* ...never saw more of the big write than the kernel reported to the
   * writer... */
  ASSERT_UINT64_LE(bytes_received - RECOVERY_SIZE, (uint64_t) writer_reported);
  /* ...and the post-cancellation write arrived intact at the very end. */
  ASSERT_OK(memcmp(tail, RECOVERY_PAYLOAD, RECOVERY_SIZE));

  CloseHandle(pended_event);
  CloseHandle(reach_event);
  CloseHandle(cancelled_event);
  CloseHandle(resume_event);

  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}

#endif /* _WIN32 */
