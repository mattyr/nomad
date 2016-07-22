package agent

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/nomad/client/allocdir"
	"github.com/hashicorp/nomad/testutil"
	"github.com/ugorji/go/codec"
)

func TestAllocDirFS_List_MissingParams(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		req, err := http.NewRequest("GET", "/v1/client/fs/ls/", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW := httptest.NewRecorder()

		_, err = s.Server.DirectoryListRequest(respW, req)
		if err != allocIDNotPresentErr {
			t.Fatalf("expected err: %v, actual: %v", allocIDNotPresentErr, err)
		}
	})
}

func TestAllocDirFS_Stat_MissingParams(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		req, err := http.NewRequest("GET", "/v1/client/fs/stat/", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW := httptest.NewRecorder()

		_, err = s.Server.FileStatRequest(respW, req)
		if err != allocIDNotPresentErr {
			t.Fatalf("expected err: %v, actual: %v", allocIDNotPresentErr, err)
		}

		req, err = http.NewRequest("GET", "/v1/client/fs/stat/foo", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW = httptest.NewRecorder()

		_, err = s.Server.FileStatRequest(respW, req)
		if err != fileNameNotPresentErr {
			t.Fatalf("expected err: %v, actual: %v", allocIDNotPresentErr, err)
		}

	})
}

func TestAllocDirFS_ReadAt_MissingParams(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		req, err := http.NewRequest("GET", "/v1/client/fs/readat/", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW := httptest.NewRecorder()

		_, err = s.Server.FileReadAtRequest(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}

		req, err = http.NewRequest("GET", "/v1/client/fs/readat/foo", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW = httptest.NewRecorder()

		_, err = s.Server.FileReadAtRequest(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}

		req, err = http.NewRequest("GET", "/v1/client/fs/readat/foo?path=/path/to/file", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW = httptest.NewRecorder()

		_, err = s.Server.FileReadAtRequest(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

type WriteCloseChecker struct {
	io.WriteCloser
	Closed bool
}

func (w *WriteCloseChecker) Close() error {
	w.Closed = true
	return w.WriteCloser.Close()
}

// This test checks, that even if the frame size has not been hit, a flush will
// periodically occur.
func TestStreamFramer_Flush(t *testing.T) {
	// Create the stream framer
	r, w := io.Pipe()
	wrappedW := &WriteCloseChecker{WriteCloser: w}
	hRate, bWindow := 100*time.Millisecond, 100*time.Millisecond
	sf := NewStreamFramer(wrappedW, hRate, bWindow, 100)
	sf.Run()

	// Create a decoder
	dec := codec.NewDecoder(r, jsonHandle)

	f := "foo"
	fe := "bar"
	d := []byte{0xa}
	o := int64(10)

	// Start the reader
	resultCh := make(chan struct{})
	go func() {
		for {
			var frame StreamFrame
			if err := dec.Decode(&frame); err != nil {
				t.Fatalf("failed to decode")
			}

			if frame.IsHeartbeat() {
				continue
			}

			if reflect.DeepEqual(frame.Data, d) && frame.Offset == o && frame.File == f && frame.FileEvent == fe {
				resultCh <- struct{}{}
				return
			}

		}
	}()

	// Write only 1 byte so we do not hit the frame size
	if err := sf.Send(f, fe, d, o); err != nil {
		t.Fatalf("Send() failed %v", err)
	}

	select {
	case <-resultCh:
	case <-time.After(2 * bWindow):
		t.Fatalf("failed to flush")
	}

	// Close the reader and wait. This should cause the runner to exit
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close reader")
	}

	select {
	case <-sf.ExitCh():
	case <-time.After(2 * hRate):
		t.Fatalf("exit channel should close")
	}

	sf.Destroy()
	if !wrappedW.Closed {
		t.Fatalf("writer not closed")
	}
}

// This test checks that frames will be batched till the frame size is hit (in
// the case that is before the flush).
func TestStreamFramer_Batch(t *testing.T) {
	// Create the stream framer
	r, w := io.Pipe()
	wrappedW := &WriteCloseChecker{WriteCloser: w}
	// Ensure the batch window doesn't get hit
	hRate, bWindow := 100*time.Millisecond, 500*time.Millisecond
	sf := NewStreamFramer(wrappedW, hRate, bWindow, 3)
	sf.Run()

	// Create a decoder
	dec := codec.NewDecoder(r, jsonHandle)

	f := "foo"
	fe := "bar"
	d := []byte{0xa, 0xb, 0xc}
	o := int64(10)

	// Start the reader
	resultCh := make(chan struct{})
	go func() {
		for {
			var frame StreamFrame
			if err := dec.Decode(&frame); err != nil {
				t.Fatalf("failed to decode")
			}

			if frame.IsHeartbeat() {
				continue
			}

			if reflect.DeepEqual(frame.Data, d) && frame.Offset == o && frame.File == f && frame.FileEvent == fe {
				resultCh <- struct{}{}
				return
			}
		}
	}()

	// Write only 1 byte so we do not hit the frame size
	if err := sf.Send(f, fe, d[:1], o); err != nil {
		t.Fatalf("Send() failed %v", err)
	}

	// Ensure we didn't get any data
	select {
	case <-resultCh:
		t.Fatalf("Got data before frame size reached")
	case <-time.After(bWindow / 2):
	}

	// Write the rest so we hit the frame size
	if err := sf.Send(f, fe, d[1:], o); err != nil {
		t.Fatalf("Send() failed %v", err)
	}

	// Ensure we get data
	select {
	case <-resultCh:
	case <-time.After(2 * bWindow):
		t.Fatalf("Did not receive data after batch size reached")
	}

	// Close the reader and wait. This should cause the runner to exit
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close reader")
	}

	select {
	case <-sf.ExitCh():
	case <-time.After(2 * hRate):
		t.Fatalf("exit channel should close")
	}

	sf.Destroy()
	if !wrappedW.Closed {
		t.Fatalf("writer not closed")
	}
}

func TestStreamFramer_Heartbeat(t *testing.T) {
	// Create the stream framer
	r, w := io.Pipe()
	wrappedW := &WriteCloseChecker{WriteCloser: w}
	hRate, bWindow := 100*time.Millisecond, 100*time.Millisecond
	sf := NewStreamFramer(wrappedW, hRate, bWindow, 100)
	sf.Run()

	// Create a decoder
	dec := codec.NewDecoder(r, jsonHandle)

	// Start the reader
	resultCh := make(chan struct{})
	go func() {
		for {
			var frame StreamFrame
			if err := dec.Decode(&frame); err != nil {
				t.Fatalf("failed to decode")
			}

			if frame.IsHeartbeat() {
				resultCh <- struct{}{}
				return
			}
		}
	}()

	select {
	case <-resultCh:
	case <-time.After(2 * hRate):
		t.Fatalf("failed to heartbeat")
	}

	// Close the reader and wait. This should cause the runner to exit
	if err := r.Close(); err != nil {
		t.Fatalf("failed to close reader")
	}

	select {
	case <-sf.ExitCh():
	case <-time.After(2 * hRate):
		t.Fatalf("exit channel should close")
	}

	sf.Destroy()
	if !wrappedW.Closed {
		t.Fatalf("writer not closed")
	}
}

func TestHTTP_Stream_MissingParams(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		req, err := http.NewRequest("GET", "/v1/client/fs/stream/", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW := httptest.NewRecorder()

		_, err = s.Server.Stream(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}

		req, err = http.NewRequest("GET", "/v1/client/fs/stream/foo", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW = httptest.NewRecorder()

		_, err = s.Server.Stream(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}

		req, err = http.NewRequest("GET", "/v1/client/fs/stream/foo?path=/path/to/file", nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		respW = httptest.NewRecorder()

		_, err = s.Server.Stream(respW, req)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

// tempAllocDir returns a new alloc dir that is rooted in a temp dir. The caller
// should destroy the temp dir.
func tempAllocDir(t *testing.T) *allocdir.AllocDir {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("TempDir() failed: %v", err)
	}

	return allocdir.NewAllocDir(dir)
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}

func TestHTTP_Stream_NoFile(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		// Get a temp alloc dir
		ad := tempAllocDir(t)
		defer os.RemoveAll(ad.AllocDir)

		if err := s.Server.stream(0, "foo", ad, nopWriteCloser{ioutil.Discard}); err == nil {
			t.Fatalf("expected an error when streaming unknown file")
		}
	})
}

func TestHTTP_Stream_Modify(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		// Get a temp alloc dir
		ad := tempAllocDir(t)
		defer os.RemoveAll(ad.AllocDir)

		// Create a file in the temp dir
		streamFile := "stream_file"
		f, err := os.Create(filepath.Join(ad.AllocDir, streamFile))
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer f.Close()

		// Create a decoder
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		dec := codec.NewDecoder(r, jsonHandle)

		data := []byte("helloworld")

		// Start the reader
		resultCh := make(chan struct{})
		go func() {
			var collected []byte
			for {
				var frame StreamFrame
				if err := dec.Decode(&frame); err != nil {
					t.Fatalf("failed to decode: %v", err)
				}

				if frame.IsHeartbeat() {
					continue
				}

				collected = append(collected, frame.Data...)
				if reflect.DeepEqual(data, collected) {
					resultCh <- struct{}{}
					return
				}
			}
		}()

		// Write a few bytes
		if _, err := f.Write(data[:3]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Start streaming
		go func() {
			if err := s.Server.stream(0, streamFile, ad, w); err != nil {
				t.Fatalf("stream() failed: %v", err)
			}
		}()

		// Sleep a little before writing more. This lets us check if the watch
		// is working.
		time.Sleep(1 * time.Second)
		if _, err := f.Write(data[3:]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		select {
		case <-resultCh:
		case <-time.After(2 * streamBatchWindow):
			t.Fatalf("failed to send new data")
		}
	})
}

func TestHTTP_Stream_Truncate(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		// Get a temp alloc dir
		ad := tempAllocDir(t)
		defer os.RemoveAll(ad.AllocDir)

		// Create a file in the temp dir
		streamFile := "stream_file"
		streamFilePath := filepath.Join(ad.AllocDir, streamFile)
		f, err := os.Create(streamFilePath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer f.Close()

		// Create a decoder
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		dec := codec.NewDecoder(r, jsonHandle)

		data := []byte("helloworld")

		// Start the reader
		truncateCh := make(chan struct{})
		dataPostTruncCh := make(chan struct{})
		go func() {
			var collected []byte
			for {
				var frame StreamFrame
				if err := dec.Decode(&frame); err != nil {
					t.Fatalf("failed to decode: %v", err)
				}

				if frame.IsHeartbeat() {
					continue
				}

				if frame.FileEvent == truncateEvent {
					close(truncateCh)
				}

				collected = append(collected, frame.Data...)
				if reflect.DeepEqual(data, collected) {
					close(dataPostTruncCh)
					return
				}
			}
		}()

		// Write a few bytes
		if _, err := f.Write(data[:3]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Start streaming
		go func() {
			if err := s.Server.stream(0, streamFile, ad, w); err != nil {
				t.Fatalf("stream() failed: %v", err)
			}
		}()

		// Sleep a little before truncating. This lets us check if the watch
		// is working.
		time.Sleep(1 * time.Second)
		if err := f.Truncate(0); err != nil {
			t.Fatalf("truncate failed: %v", err)
		}
		if err := f.Sync(); err != nil {
			t.Fatalf("sync failed: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("failed to close file: %v", err)
		}

		f2, err := os.OpenFile(streamFilePath, os.O_RDWR, 0)
		if err != nil {
			t.Fatalf("failed to reopen file: %v", err)
		}
		defer f2.Close()
		if _, err := f2.Write(data[3:5]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		select {
		case <-truncateCh:
		case <-time.After(2 * streamBatchWindow):
			t.Fatalf("did not receive truncate")
		}

		// Sleep a little before writing more. This lets us check if the watch
		// is working.
		time.Sleep(1 * time.Second)
		if _, err := f2.Write(data[5:]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		select {
		case <-dataPostTruncCh:
		case <-time.After(2 * streamBatchWindow):
			t.Fatalf("did not receive post truncate data")
		}
	})
}

func TestHTTP_Stream_Delete(t *testing.T) {
	httpTest(t, nil, func(s *TestServer) {
		// Get a temp alloc dir
		ad := tempAllocDir(t)
		defer os.RemoveAll(ad.AllocDir)

		// Create a file in the temp dir
		streamFile := "stream_file"
		streamFilePath := filepath.Join(ad.AllocDir, streamFile)
		f, err := os.Create(streamFilePath)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		defer f.Close()

		// Create a decoder
		r, w := io.Pipe()
		wrappedW := &WriteCloseChecker{WriteCloser: w}
		defer r.Close()
		defer w.Close()
		dec := codec.NewDecoder(r, jsonHandle)

		data := []byte("helloworld")

		// Start the reader
		deleteCh := make(chan struct{})
		go func() {
			for {
				var frame StreamFrame
				if err := dec.Decode(&frame); err != nil {
					t.Fatalf("failed to decode: %v", err)
				}

				if frame.IsHeartbeat() {
					continue
				}

				if frame.FileEvent == deleteEvent {
					close(deleteCh)
					return
				}
			}
		}()

		// Write a few bytes
		if _, err := f.Write(data[:3]); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Start streaming
		go func() {
			if err := s.Server.stream(0, streamFile, ad, wrappedW); err != nil {
				t.Fatalf("stream() failed: %v", err)
			}
		}()

		// Sleep a little before deleting. This lets us check if the watch
		// is working.
		time.Sleep(1 * time.Second)
		if err := os.Remove(streamFilePath); err != nil {
			t.Fatalf("delete failed: %v", err)
		}

		select {
		case <-deleteCh:
		case <-time.After(4 * streamBatchWindow):
			t.Fatalf("did not receive delete")
		}

		testutil.WaitForResult(func() (bool, error) {
			return wrappedW.Closed, nil
		}, func(err error) {
			t.Fatalf("connection not closed")
		})

	})
}
