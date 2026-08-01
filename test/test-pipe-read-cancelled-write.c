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
 * advertised-but-unread bytes are retracted. A reader that sizes a blocking
 * read from a stale peek then wedges its event loop forever. This test parks
 * the reader's loop mid-drain (inside read_cb) while a raw Win32 writer
 * thread cancels its own pended overlapped WriteFile, and then asserts that
 * the loop stays live (a repeating timer keeps firing), that data written
 * after the cancellation is still delivered intact, and that closing the
 * write end yields a clean EOF.
 */

#include "uv.h"
#include "task.h"

#include <stdio.h>
#include <string.h>

#ifndef _WIN32

TEST_IMPL(pipe_read_cancelled_write) {
  RETURN_SKIP("Windows-specific pipe write-retraction test.");
}

TEST_IMPL(pipe_read_cancelled_write_ipc) {
  RETURN_SKIP("Windows-specific pipe write-retraction test.");
}

TEST_IMPL(pipe_read_ipc_split_xfer) {
  RETURN_SKIP("Windows-specific pipe write-retraction test.");
}

TEST_IMPL(pipe_read_restart_allocator) {
  RETURN_SKIP("Windows-specific pipe write-retraction test.");
}

TEST_IMPL(pipe_write_cancel_ipc_enotsup) {
  RETURN_SKIP("Windows-specific pipe write-retraction test.");
}

#else /* _WIN32 */

#include <io.h>

#define BIG_WRITE_SIZE (64 * 1024 * 1024)
#define READ_PARK_THRESHOLD (8 * 1024 * 1024)
#define RECOVERY_PAYLOAD "PIPE-RECOVERED!!"
#define RECOVERY_SIZE (sizeof(RECOVERY_PAYLOAD) - 1)
#define POST_CANCEL_TICKS 5

static uv_pipe_t reader;
static uv_timer_t liveness_timer;
static uv_thread_t writer_thread;

static HANDLE write_end;
static int write_fd = -1;
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

  /* EOF for the reader. The handle came from _get_osfhandle, so the CRT
   * descriptor owns it; close the descriptor, not the raw handle. */
  ASSERT_OK(_close(write_fd));
  write_fd = -1;
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
    /* Park the loop mid-delivery while the writer thread cancels its
     * in-flight write. When this returns, the bytes the pipe advertised a
     * moment ago are guaranteed retracted; the read posted after this
     * callback must simply stay pending until new data arrives. */
    ASSERT_NE(0, SetEvent(reach_event));
    ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(cancelled_event, 30000));
    park_over = 1;
  }
}


static void timer_cb(uv_timer_t* timer) {
  if (park_over && !loop_proven_live &&
      ++post_cancel_ticks >= POST_CANCEL_TICKS) {
    /* The loop kept turning after the retraction: the reader did not wedge.
     * Let the writer finish (recovery write + EOF). */
    loop_proven_live = 1;
    ASSERT_NE(0, SetEvent(resume_event));
  }
}


TEST_IMPL(pipe_read_cancelled_write) {
  uv_file fds[2];

  /* Both ends overlapped ("non-blocking"). */
  ASSERT_OK(uv_pipe(fds, UV_NONBLOCK_PIPE, UV_NONBLOCK_PIPE));

  /* The write end is driven with raw Win32 overlapped I/O. */
  write_fd = fds[1];
  write_end = (HANDLE) _get_osfhandle(fds[1]);
  ASSERT_PTR_NE(write_end, INVALID_HANDLE_VALUE);

  pended_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  reach_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  cancelled_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  resume_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(pended_event);
  ASSERT_NOT_NULL(reach_event);
  ASSERT_NOT_NULL(cancelled_event);
  ASSERT_NOT_NULL(resume_event);

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &reader, 0));
  ASSERT_OK(uv_pipe_open(&reader, fds[0]));

  ASSERT_OK(uv_timer_init(uv_default_loop(), &liveness_timer));
  ASSERT_OK(uv_timer_start(&liveness_timer, timer_cb, 10, 10));

  /* Only start reading once the big write is already in flight, so that
   * the posted read immediately starts consuming its advertised bytes. */
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

  /* The cancellation truncated the big write: the reader saw a strict
   * prefix of it, not the whole thing. (Note that the transfer count the
   * kernel reports for the aborted WriteFile cannot be used for accounting
   * here: the named pipe file system reports 0 for a cancelled pended
   * write even when the reader has already consumed a prefix of it.) */
  ASSERT_LT(writer_reported, (DWORD) BIG_WRITE_SIZE);
  ASSERT_UINT64_LT(bytes_received - RECOVERY_SIZE, (uint64_t) BIG_WRITE_SIZE);
  /* The reader consumed the park threshold plus the recovery payload... */
  ASSERT_UINT64_GE(bytes_received, READ_PARK_THRESHOLD + RECOVERY_SIZE);
  /* ...and the post-cancellation write arrived intact at the very end. */
  ASSERT_OK(memcmp(tail, RECOVERY_PAYLOAD, RECOVERY_SIZE));

  CloseHandle(pended_event);
  CloseHandle(reach_event);
  CloseHandle(cancelled_event);
  CloseHandle(resume_event);

  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


/* Deterministic reproduction of the IPC read-loop spin: a raw writer thread
 * pends one large hand-crafted IPC data frame; the libuv IPC reader parks
 * its loop mid-payload (inside read_cb) while the writer cancels the write
 * with CancelIoEx, retracting the advertised payload. Before the fix, the
 * read machinery then called back into the frame reader forever (the
 * remaining payload count was nonzero but nothing could be read), spinning
 * the loop at 100% CPU; this test timed out. Fixed, the reader's bounded
 * payload read simply stays pending - the loop live and quiescent - and
 * closing the write end delivers a clean EOF through it. */

#define IPC_PAYLOAD_SIZE (64 * 1024 * 1024)
#define IPC_CANCEL_THRESHOLD (8 * 1024 * 1024)

/* Mirrors libuv's private IPC frame header (see src/win/pipe.c); flags 0x01
 * is "frame carries data". */
typedef struct {
  uint32_t flags;
  uint32_t reserved1;
  uint32_t data_length;
  uint32_t reserved2;
} ipc_frame_header_t;

static uv_pipe_t ipc_reader;
static uint64_t ipc_bytes_received;
static int ipc_eof_seen;
static int ipc_close_cb_called;


static void ipc_close_cb(uv_handle_t* handle) {
  ipc_close_cb_called++;
}


static void ipc_alloc_cb(uv_handle_t* handle,
                         size_t suggested_size,
                         uv_buf_t* buf) {
  buf->base = read_slab;
  buf->len = sizeof(read_slab);
}


static void ipc_writer_thread_proc(void* arg) {
  char* frame;
  ipc_frame_header_t* header;
  OVERLAPPED ov;
  DWORD transferred;
  BOOL r;

  frame = (char*) malloc(sizeof(*header) + IPC_PAYLOAD_SIZE);
  ASSERT_NOT_NULL(frame);
  header = (ipc_frame_header_t*) frame;
  memset(frame, 0xCD, sizeof(*header) + IPC_PAYLOAD_SIZE);
  header->flags = 0x01; /* UV__IPC_FRAME_HAS_DATA */
  header->reserved1 = 0;
  header->data_length = IPC_PAYLOAD_SIZE;
  header->reserved2 = 0;

  memset(&ov, 0, sizeof(ov));
  ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(ov.hEvent);

  r = WriteFile(write_end,
                frame,
                (DWORD) (sizeof(*header) + IPC_PAYLOAD_SIZE),
                NULL,
                &ov);
  ASSERT_EQ(0, r);
  ASSERT_EQ((DWORD) ERROR_IO_PENDING, GetLastError());
  ASSERT_NE(0, SetEvent(pended_event));

  /* Wait until the reader is parked mid-payload, then retract the rest. */
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(reach_event, 30000));
  CancelIoEx(write_end, &ov);
  if (!GetOverlappedResult(write_end, &ov, &transferred, TRUE))
    ASSERT_EQ((DWORD) ERROR_OPERATION_ABORTED, GetLastError());
  ASSERT_NE(0, SetEvent(cancelled_event));

  /* Wait for the loop to prove it settled, then EOF the reader. */
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(resume_event, 30000));
  CloseHandle(ov.hEvent);
  CloseHandle(write_end);
  write_end = INVALID_HANDLE_VALUE;
  free(frame);
}


static void ipc_read_cb(uv_stream_t* stream,
                        ssize_t nread,
                        const uv_buf_t* buf) {
  if (nread == UV_EOF) {
    ipc_eof_seen = 1;
    uv_close((uv_handle_t*) &ipc_reader, ipc_close_cb);
    uv_close((uv_handle_t*) &liveness_timer, ipc_close_cb);
    return;
  }

  ASSERT_GE(nread, 0);
  ipc_bytes_received += (uint64_t) nread;

  if (ipc_bytes_received >= IPC_CANCEL_THRESHOLD && !reach_signaled) {
    reach_signaled = 1;
    /* Park the loop mid-drain while the writer retracts the payload. */
    ASSERT_NE(0, SetEvent(reach_event));
    ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(cancelled_event, 30000));
    park_over = 1;
  }
}


TEST_IMPL(pipe_read_cancelled_write_ipc) {
  char name[128];
  HANDLE srv;
  OVERLAPPED conn_ov;
  DWORD conn_err;

  /* IPC pipes must be duplex; build a raw duplex overlapped pair. The
   * reader end goes to libuv, the writer end stays raw. */
  snprintf(name,
           sizeof(name),
           "\\\\.\\pipe\\uv-read-cancelled-write-ipc-%lu",
           (unsigned long) GetCurrentProcessId());
  srv = CreateNamedPipeA(name,
                         PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED |
                             FILE_FLAG_FIRST_PIPE_INSTANCE,
                         PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                         1,
                         65536,
                         65536,
                         0,
                         NULL);
  ASSERT_PTR_NE(srv, INVALID_HANDLE_VALUE);
  write_end = CreateFileA(name,
                          GENERIC_READ | GENERIC_WRITE,
                          0,
                          NULL,
                          OPEN_EXISTING,
                          FILE_FLAG_OVERLAPPED,
                          NULL);
  ASSERT_PTR_NE(write_end, INVALID_HANDLE_VALUE);
  memset(&conn_ov, 0, sizeof(conn_ov));
  conn_ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(conn_ov.hEvent);
  if (!ConnectNamedPipe(srv, &conn_ov)) {
    conn_err = GetLastError();
    if (conn_err == ERROR_IO_PENDING)
      ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(conn_ov.hEvent, 10000));
    else
      ASSERT_EQ((DWORD) ERROR_PIPE_CONNECTED, conn_err);
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

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &ipc_reader, 1));
  ASSERT_OK(uv_pipe_open(&ipc_reader, _open_osfhandle((intptr_t) srv, 0)));

  ASSERT_OK(uv_timer_init(uv_default_loop(), &liveness_timer));
  ASSERT_OK(uv_timer_start(&liveness_timer, timer_cb, 10, 10));

  ASSERT_OK(uv_thread_create(&writer_thread, ipc_writer_thread_proc, NULL));
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(pended_event, 30000));
  ASSERT_OK(uv_read_start((uv_stream_t*) &ipc_reader,
                          ipc_alloc_cb,
                          ipc_read_cb));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));
  ASSERT_OK(uv_thread_join(&writer_thread));

  ASSERT_EQ(1, reach_signaled);
  ASSERT_EQ(1, park_over);
  ASSERT_EQ(1, loop_proven_live); /* Timer kept firing after retraction. */
  ASSERT_EQ(1, ipc_eof_seen);
  ASSERT_EQ(2, ipc_close_cb_called);
  /* The reader saw a strict prefix of the retracted payload. */
  ASSERT_UINT64_GE(ipc_bytes_received, IPC_CANCEL_THRESHOLD);
  ASSERT_UINT64_LT(ipc_bytes_received, (uint64_t) IPC_PAYLOAD_SIZE);

  CloseHandle(pended_event);
  CloseHandle(reach_event);
  CloseHandle(cancelled_event);
  CloseHandle(resume_event);

  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


/* A socket-transfer record split across two writes must be reassembled, and
 * must take priority over the payload that its own frame header already
 * announced: the first write ends 100 bytes into the 632-byte record, so
 * when the rest arrives, a reader that consulted the payload count first
 * would consume the record's remainder as application payload and corrupt
 * the stream. The record content itself is garbage, which is fine as long
 * as nothing accepts the pending handle. */

#define SPLIT_XFER_RECORD_SIZE 632
#define SPLIT_XFER_PART1 100
#define SPLIT_PAYLOAD_SIZE 64
#define SPLIT_TRAILER "TRAILER!"

static uv_pipe_t split_reader;
static char split_received[SPLIT_PAYLOAD_SIZE + sizeof(SPLIT_TRAILER)];
static size_t split_received_len;
static int split_eof_seen;
static int split_close_cb_called;


static void split_close_cb(uv_handle_t* handle) {
  split_close_cb_called++;
}


static void split_read_cb(uv_stream_t* stream,
                          ssize_t nread,
                          const uv_buf_t* buf) {
  ssize_t i;

  if (nread == UV_EOF) {
    split_eof_seen = 1;
    uv_close((uv_handle_t*) &split_reader, split_close_cb);
    return;
  }

  ASSERT_GE(nread, 0);
  for (i = 0; i < nread; i++) {
    ASSERT_LT(split_received_len, sizeof(split_received));
    split_received[split_received_len++] = buf->base[i];
  }
}


static void split_writer_thread_proc(void* arg) {
  ipc_frame_header_t header;
  char record[SPLIT_XFER_RECORD_SIZE];
  char part1[sizeof(header) + SPLIT_XFER_PART1];
  char part2[SPLIT_XFER_RECORD_SIZE - SPLIT_XFER_PART1 + SPLIT_PAYLOAD_SIZE +
             sizeof(header) + sizeof(SPLIT_TRAILER) - 1];
  char* q;
  OVERLAPPED ov;
  DWORD transferred;

  memset(record, 0x5C, sizeof(record));

  /* Frame 1: HAS_DATA | HAS_SOCKET_XFER, 64 payload bytes announced. */
  header.flags = 0x03;
  header.reserved1 = 0;
  header.data_length = SPLIT_PAYLOAD_SIZE;
  header.reserved2 = 0;

  memcpy(part1, &header, sizeof(header));
  memcpy(part1 + sizeof(header), record, SPLIT_XFER_PART1);

  q = part2;
  memcpy(q, record + SPLIT_XFER_PART1,
         SPLIT_XFER_RECORD_SIZE - SPLIT_XFER_PART1);
  q += SPLIT_XFER_RECORD_SIZE - SPLIT_XFER_PART1;
  memset(q, 'P', SPLIT_PAYLOAD_SIZE);
  q += SPLIT_PAYLOAD_SIZE;
  /* Frame 2: plain data frame carrying the trailer. */
  header.flags = 0x01;
  header.data_length = sizeof(SPLIT_TRAILER) - 1;
  memcpy(q, &header, sizeof(header));
  q += sizeof(header);
  memcpy(q, SPLIT_TRAILER, sizeof(SPLIT_TRAILER) - 1);

  memset(&ov, 0, sizeof(ov));
  ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(ov.hEvent);

  if (!WriteFile(write_end, part1, (DWORD) sizeof(part1), NULL, &ov))
    ASSERT_EQ((DWORD) ERROR_IO_PENDING, GetLastError());
  ASSERT_NE(0, GetOverlappedResult(write_end, &ov, &transferred, TRUE));
  ASSERT_EQ((DWORD) sizeof(part1), transferred);

  /* Give the reader time to observe the record running dry mid-way. (Not
   * required for correctness - only for the test to exercise the
   * reassembly path rather than reading everything in one go.) */
  ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(reach_event, 30000));

  ASSERT_NE(0, ResetEvent(ov.hEvent));
  if (!WriteFile(write_end, part2, (DWORD) sizeof(part2), NULL, &ov))
    ASSERT_EQ((DWORD) ERROR_IO_PENDING, GetLastError());
  ASSERT_NE(0, GetOverlappedResult(write_end, &ov, &transferred, TRUE));
  ASSERT_EQ((DWORD) sizeof(part2), transferred);

  CloseHandle(ov.hEvent);
  CloseHandle(write_end);
  write_end = INVALID_HANDLE_VALUE;
}


static void split_pause_timer_cb(uv_timer_t* timer) {
  /* By now the reader has drained part1: its pending read is parked
   * mid-record, waiting for the record's remainder. */
  ASSERT_NE(0, SetEvent(reach_event));
  uv_close((uv_handle_t*) timer, split_close_cb);
}


TEST_IMPL(pipe_read_ipc_split_xfer) {
  char name[128];
  HANDLE srv;
  OVERLAPPED conn_ov;
  DWORD conn_err;

  snprintf(name,
           sizeof(name),
           "\\\\.\\pipe\\uv-split-xfer-%lu",
           (unsigned long) GetCurrentProcessId());
  srv = CreateNamedPipeA(name,
                         PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED |
                             FILE_FLAG_FIRST_PIPE_INSTANCE,
                         PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                         1,
                         65536,
                         65536,
                         0,
                         NULL);
  ASSERT_PTR_NE(srv, INVALID_HANDLE_VALUE);
  write_end = CreateFileA(name,
                          GENERIC_READ | GENERIC_WRITE,
                          0,
                          NULL,
                          OPEN_EXISTING,
                          FILE_FLAG_OVERLAPPED,
                          NULL);
  ASSERT_PTR_NE(write_end, INVALID_HANDLE_VALUE);
  memset(&conn_ov, 0, sizeof(conn_ov));
  conn_ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(conn_ov.hEvent);
  if (!ConnectNamedPipe(srv, &conn_ov)) {
    conn_err = GetLastError();
    if (conn_err == ERROR_IO_PENDING)
      ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(conn_ov.hEvent, 10000));
    else
      ASSERT_EQ((DWORD) ERROR_PIPE_CONNECTED, conn_err);
  }
  CloseHandle(conn_ov.hEvent);

  reach_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(reach_event);

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &split_reader, 1));
  ASSERT_OK(uv_pipe_open(&split_reader, _open_osfhandle((intptr_t) srv, 0)));
  ASSERT_OK(uv_read_start((uv_stream_t*) &split_reader,
                          ipc_alloc_cb,
                          split_read_cb));

  ASSERT_OK(uv_timer_init(uv_default_loop(), &liveness_timer));
  ASSERT_OK(uv_timer_start(&liveness_timer, split_pause_timer_cb, 100, 0));

  ASSERT_OK(uv_thread_create(&writer_thread, split_writer_thread_proc, NULL));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));
  ASSERT_OK(uv_thread_join(&writer_thread));

  ASSERT_EQ(1, split_eof_seen);
  ASSERT_EQ(2, split_close_cb_called);
  /* Exactly the announced payload plus the trailer frame's payload arrived,
   * in order: none of the record's bytes leaked into the data stream. */
  ASSERT_EQ(SPLIT_PAYLOAD_SIZE + sizeof(SPLIT_TRAILER) - 1,
            split_received_len);
  {
    int i;
    for (i = 0; i < SPLIT_PAYLOAD_SIZE; i++)
      ASSERT_EQ('P', split_received[i]);
  }
  ASSERT_OK(memcmp(split_received + SPLIT_PAYLOAD_SIZE,
                   SPLIT_TRAILER,
                   sizeof(SPLIT_TRAILER) - 1));

  CloseHandle(reach_event);
  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


/* uv_read_stop() followed by uv_read_start() with a different allocator,
 * while a read posted from the first allocator is still pending.
 *
 * Overlapped Windows pipe reads follow the TCP model: there is exactly one
 * outstanding read, posted into an alloc_cb buffer, and uv_read_stop() only
 * stops delivery - it does not cancel. A read that is already pending
 * survives the stop, stays pinned to the (first-generation) buffer it was
 * posted with, and is delivered to whichever read_cb is current when it
 * completes.
 * The contract asserted here: every delivery lands in one of the caller's
 * two known buffers, the byte stream is exact (nothing lost, duplicated or
 * reordered across the restart), the first delivery after the restart
 * arrives in the first allocator's buffer (that is the read that was
 * pending across the stop), and later deliveries use the new allocator. */

#define RESTART_TOTAL (256 * 1024)
#define RESTART_SWITCH_AT (64 * 1024)

static uv_timer_t restart_timer;
static HANDLE restart_resume_event; /* timer_cb -> writer: restart done */
static char restart_buf_a[4096];
static char restart_buf_b[4096];
static uint64_t restart_received;
static int restart_timer_started;
static int restart_switched;
static int restart_b_deliveries;
static int restart_first_b_cb_seen;
static int restart_eof_seen;
static int restart_close_cb_called;


static void restart_close_cb(uv_handle_t* handle) {
  restart_close_cb_called++;
}


static void restart_alloc_a(uv_handle_t* handle,
                            size_t suggested_size,
                            uv_buf_t* buf) {
  ASSERT_EQ(0, restart_switched);
  buf->base = restart_buf_a;
  buf->len = sizeof(restart_buf_a);
}


static void restart_alloc_b(uv_handle_t* handle,
                            size_t suggested_size,
                            uv_buf_t* buf) {
  ASSERT_EQ(1, restart_switched);
  buf->base = restart_buf_b;
  buf->len = sizeof(restart_buf_b);
}


static void restart_verify(const uv_buf_t* buf, ssize_t nread) {
  ssize_t i;
  for (i = 0; i < nread; i++)
    ASSERT_EQ((char) ((restart_received + i) & 0xff), buf->base[i]);
}


static void restart_read_cb_b(uv_stream_t* stream,
                              ssize_t nread,
                              const uv_buf_t* buf) {
  if (nread == UV_EOF) {
    restart_eof_seen = 1;
    uv_close((uv_handle_t*) &reader, restart_close_cb);
    return;
  }
  ASSERT_GE(nread, 0);
  if (nread > 0) {
    /* Every delivery must use one of the two known buffers. The read that
     * was pending across the stop was posted with allocator A's buffer and
     * survives the restart, so the first delivery here must arrive in it;
     * reads posted after the restart use allocator B's. */
    if (!restart_first_b_cb_seen) {
      restart_first_b_cb_seen = 1;
      ASSERT_PTR_EQ(buf->base, restart_buf_a);
    } else {
      ASSERT_PTR_EQ(buf->base, restart_buf_b);
      restart_b_deliveries++;
    }
  }
  restart_verify(buf, nread);
  restart_received += (uint64_t) nread;
}


static void restart_switch_timer_cb(uv_timer_t* timer) {
  /* The writer has paused: the first phase has been consumed in full and a
   * read posted with allocator A's buffer is pending on a dry pipe. Stop
   * and restart with the second-generation allocator, then let the writer
   * continue. The pending read must survive this untouched. */
  ASSERT_UINT64_EQ(RESTART_SWITCH_AT, restart_received);
  restart_switched = 1;
  ASSERT_OK(uv_read_stop((uv_stream_t*) &reader));
  ASSERT_OK(uv_read_start((uv_stream_t*) &reader,
                          restart_alloc_b,
                          restart_read_cb_b));
  uv_close((uv_handle_t*) timer, restart_close_cb);
  ASSERT_NE(0, SetEvent(restart_resume_event));
}


static void restart_read_cb_a(uv_stream_t* stream,
                              ssize_t nread,
                              const uv_buf_t* buf) {
  ASSERT_GE(nread, 0);
  if (nread > 0)
    ASSERT_PTR_EQ(buf->base, restart_buf_a);
  restart_verify(buf, nread);
  restart_received += (uint64_t) nread;

  if (restart_received == RESTART_SWITCH_AT && !restart_timer_started) {
    /* First phase fully consumed; the writer is now waiting. Switch
     * allocators from a timer, outside read_cb, so that the switch happens
     * with a read pending (posted right after this callback returns). */
    restart_timer_started = 1;
    ASSERT_OK(uv_timer_start(&restart_timer, restart_switch_timer_cb, 10, 0));
  }
}


static void restart_writer_thread_proc(void* arg) {
  char chunk[4096];
  size_t off;
  DWORD written;
  size_t i;

  for (off = 0; off < RESTART_TOTAL; off += sizeof(chunk)) {
    if (off == RESTART_SWITCH_AT) {
      /* Pause with the pipe dry until the reader has switched allocators,
       * so that the switch provably happens with the read pending. */
      ASSERT_EQ(WAIT_OBJECT_0,
                WaitForSingleObject(restart_resume_event, 30000));
    }
    for (i = 0; i < sizeof(chunk); i++)
      chunk[i] = (char) ((off + i) & 0xff);
    ASSERT_NE(0, WriteFile(write_end, chunk, sizeof(chunk), &written, NULL));
    ASSERT_EQ((DWORD) sizeof(chunk), written);
  }
  ASSERT_OK(_close(write_fd));
  write_fd = -1;
  write_end = INVALID_HANDLE_VALUE;
}


TEST_IMPL(pipe_read_restart_allocator) {
  uv_file fds[2];

  /* Overlapped ("non-blocking") read end: only overlapped pipes carry the
   * TCP-style posted read that survives uv_read_stop(). Pipes in
   * non-overlapped mode use a bufferless doorbell read that stopping
   * cancels, as upstream always has. */
  ASSERT_OK(uv_pipe(fds, UV_NONBLOCK_PIPE, 0));
  write_fd = fds[1];
  write_end = (HANDLE) _get_osfhandle(fds[1]);
  ASSERT_PTR_NE(write_end, INVALID_HANDLE_VALUE);

  restart_resume_event = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(restart_resume_event);

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &reader, 0));
  ASSERT_OK(uv_pipe_open(&reader, fds[0]));
  ASSERT_OK(uv_timer_init(uv_default_loop(), &restart_timer));
  ASSERT_OK(uv_read_start((uv_stream_t*) &reader,
                          restart_alloc_a,
                          restart_read_cb_a));

  ASSERT_OK(uv_thread_create(&writer_thread,
                             restart_writer_thread_proc,
                             NULL));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));
  ASSERT_OK(uv_thread_join(&writer_thread));

  ASSERT_EQ(1, restart_switched);
  ASSERT_EQ(1, restart_first_b_cb_seen);
  ASSERT_GT(restart_b_deliveries, 0); /* Allocator B took over. */
  ASSERT_EQ(1, restart_eof_seen);
  ASSERT_EQ(2, restart_close_cb_called);
  ASSERT_UINT64_EQ(RESTART_TOTAL, restart_received); /* Nothing lost. */

  CloseHandle(restart_resume_event);

  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}


/* uv_cancel() on an IPC pipe write is refused (UV_ENOTSUP): a truncated
 * frame would permanently desynchronize the receiver. The write must then
 * complete normally, and uv_write_nwritten() must report the application
 * payload only (excluding libuv's frame header). */

#define ENOTSUP_PAYLOAD (1024 * 1024)

static uv_pipe_t enotsup_reader;
static uv_pipe_t enotsup_writer;
static uv_write_t enotsup_write_req;
static char* enotsup_data;
static uint64_t enotsup_received;
static int enotsup_write_cb_called;
static int enotsup_close_cb_called;


static void enotsup_close_cb(uv_handle_t* handle) {
  enotsup_close_cb_called++;
}


static void enotsup_write_cb(uv_write_t* req, int status) {
  ASSERT_OK(status);
  ASSERT_UINT64_EQ(ENOTSUP_PAYLOAD, uv_write_nwritten(req));
  enotsup_write_cb_called++;
  uv_close((uv_handle_t*) &enotsup_writer, enotsup_close_cb);
}


static void enotsup_read_cb(uv_stream_t* stream,
                            ssize_t nread,
                            const uv_buf_t* buf) {
  if (nread == UV_EOF) {
    uv_close((uv_handle_t*) &enotsup_reader, enotsup_close_cb);
    return;
  }
  ASSERT_GE(nread, 0);
  enotsup_received += (uint64_t) nread;
}


TEST_IMPL(pipe_write_cancel_ipc_enotsup) {
  char name[128];
  HANDLE srv;
  HANDLE cli;
  OVERLAPPED conn_ov;
  DWORD conn_err;
  uv_buf_t buf;

  snprintf(name,
           sizeof(name),
           "\\\\.\\pipe\\uv-ipc-enotsup-%lu",
           (unsigned long) GetCurrentProcessId());
  srv = CreateNamedPipeA(name,
                         PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED |
                             FILE_FLAG_FIRST_PIPE_INSTANCE,
                         PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT,
                         1,
                         65536,
                         65536,
                         0,
                         NULL);
  ASSERT_PTR_NE(srv, INVALID_HANDLE_VALUE);
  cli = CreateFileA(name,
                    GENERIC_READ | GENERIC_WRITE,
                    0,
                    NULL,
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED,
                    NULL);
  ASSERT_PTR_NE(cli, INVALID_HANDLE_VALUE);
  memset(&conn_ov, 0, sizeof(conn_ov));
  conn_ov.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NOT_NULL(conn_ov.hEvent);
  if (!ConnectNamedPipe(srv, &conn_ov)) {
    conn_err = GetLastError();
    if (conn_err == ERROR_IO_PENDING)
      ASSERT_EQ(WAIT_OBJECT_0, WaitForSingleObject(conn_ov.hEvent, 10000));
    else
      ASSERT_EQ((DWORD) ERROR_PIPE_CONNECTED, conn_err);
  }
  CloseHandle(conn_ov.hEvent);

  ASSERT_OK(uv_pipe_init(uv_default_loop(), &enotsup_reader, 1));
  ASSERT_OK(uv_pipe_open(&enotsup_reader, _open_osfhandle((intptr_t) srv, 0)));
  ASSERT_OK(uv_pipe_init(uv_default_loop(), &enotsup_writer, 1));
  ASSERT_OK(uv_pipe_open(&enotsup_writer, _open_osfhandle((intptr_t) cli, 0)));

  enotsup_data = (char*) malloc(ENOTSUP_PAYLOAD);
  ASSERT_NOT_NULL(enotsup_data);
  memset(enotsup_data, 0xEE, ENOTSUP_PAYLOAD);

  buf = uv_buf_init(enotsup_data, ENOTSUP_PAYLOAD);
  ASSERT_OK(uv_write(&enotsup_write_req,
                     (uv_stream_t*) &enotsup_writer,
                     &buf,
                     1,
                     enotsup_write_cb));

  /* In flight: cancellation must be refused. */
  ASSERT_EQ(UV_ENOTSUP, uv_cancel((uv_req_t*) &enotsup_write_req));

  ASSERT_OK(uv_read_start((uv_stream_t*) &enotsup_reader,
                          ipc_alloc_cb,
                          enotsup_read_cb));

  ASSERT_OK(uv_run(uv_default_loop(), UV_RUN_DEFAULT));

  ASSERT_EQ(1, enotsup_write_cb_called);
  ASSERT_EQ(2, enotsup_close_cb_called);
  ASSERT_UINT64_EQ(ENOTSUP_PAYLOAD, enotsup_received);

  free(enotsup_data);
  MAKE_VALGRIND_HAPPY(uv_default_loop());
  return 0;
}

#endif /* _WIN32 */
