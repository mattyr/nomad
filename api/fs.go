package api

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	// OriginStart and OriginEnd are the available parameters for the origin
	// argument when streaming a file. They respectively offset from the start
	// and end of a file.
	OriginStart = "start"
	OriginEnd   = "end"
)

// AllocFileInfo holds information about a file inside the AllocDir
type AllocFileInfo struct {
	Name     string
	IsDir    bool
	Size     int64
	FileMode string
	ModTime  time.Time
}

// StreamFrame is used to frame data of a file when streaming
type StreamFrame struct {
	Offset    int64  `json:",omitempty"`
	Data      []byte `json:",omitempty"`
	File      string `json:",omitempty"`
	FileEvent string `json:",omitempty"`
}

// IsHeartbeat returns if the frame is a heartbeat frame
func (s *StreamFrame) IsHeartbeat() bool {
	return len(s.Data) == 0 && s.FileEvent == "" && s.File == "" && s.Offset == 0
}

// AllocFS is used to introspect an allocation directory on a Nomad client
type AllocFS struct {
	client *Client
}

// AllocFS returns an handle to the AllocFS endpoints
func (c *Client) AllocFS() *AllocFS {
	return &AllocFS{client: c}
}

// List is used to list the files at a given path of an allocation directory
func (a *AllocFS) List(alloc *Allocation, path string, q *QueryOptions) ([]*AllocFileInfo, *QueryMeta, error) {
	node, _, err := a.client.Nodes().Info(alloc.NodeID, &QueryOptions{})
	if err != nil {
		return nil, nil, err
	}

	if node.HTTPAddr == "" {
		return nil, nil, fmt.Errorf("http addr of the node where alloc %q is running is not advertised", alloc.ID)
	}
	u := &url.URL{
		Scheme: "http",
		Host:   node.HTTPAddr,
		Path:   fmt.Sprintf("/v1/client/fs/ls/%s", alloc.ID),
	}
	v := url.Values{}
	v.Set("path", path)
	u.RawQuery = v.Encode()
	req := &http.Request{
		Method: "GET",
		URL:    u,
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, a.getErrorMsg(resp)
	}
	decoder := json.NewDecoder(resp.Body)
	var files []*AllocFileInfo
	if err := decoder.Decode(&files); err != nil {
		return nil, nil, err
	}
	return files, nil, nil
}

// Stat is used to stat a file at a given path of an allocation directory
func (a *AllocFS) Stat(alloc *Allocation, path string, q *QueryOptions) (*AllocFileInfo, *QueryMeta, error) {
	node, _, err := a.client.Nodes().Info(alloc.NodeID, &QueryOptions{})
	if err != nil {
		return nil, nil, err
	}

	if node.HTTPAddr == "" {
		return nil, nil, fmt.Errorf("http addr of the node where alloc %q is running is not advertised", alloc.ID)
	}
	u := &url.URL{
		Scheme: "http",
		Host:   node.HTTPAddr,
		Path:   fmt.Sprintf("/v1/client/fs/stat/%s", alloc.ID),
	}
	v := url.Values{}
	v.Set("path", path)
	u.RawQuery = v.Encode()
	req := &http.Request{
		Method: "GET",
		URL:    u,
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != 200 {
		return nil, nil, a.getErrorMsg(resp)
	}
	decoder := json.NewDecoder(resp.Body)
	var file *AllocFileInfo
	if err := decoder.Decode(&file); err != nil {
		return nil, nil, err
	}
	return file, nil, nil
}

// ReadAt is used to read bytes at a given offset until limit at the given path
// in an allocation directory. If limit is <= 0, there is no limit.
func (a *AllocFS) ReadAt(alloc *Allocation, path string, offset int64, limit int64, q *QueryOptions) (io.ReadCloser, *QueryMeta, error) {
	node, _, err := a.client.Nodes().Info(alloc.NodeID, &QueryOptions{})
	if err != nil {
		return nil, nil, err
	}

	if node.HTTPAddr == "" {
		return nil, nil, fmt.Errorf("http addr of the node where alloc %q is running is not advertised", alloc.ID)
	}
	u := &url.URL{
		Scheme: "http",
		Host:   node.HTTPAddr,
		Path:   fmt.Sprintf("/v1/client/fs/readat/%s", alloc.ID),
	}
	v := url.Values{}
	v.Set("path", path)
	v.Set("offset", strconv.FormatInt(offset, 10))
	v.Set("limit", strconv.FormatInt(limit, 10))
	u.RawQuery = v.Encode()
	req := &http.Request{
		Method: "GET",
		URL:    u,
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, nil, nil
}

// Cat is used to read contents of a file at the given path in an allocation
// directory
func (a *AllocFS) Cat(alloc *Allocation, path string, q *QueryOptions) (io.ReadCloser, *QueryMeta, error) {
	node, _, err := a.client.Nodes().Info(alloc.NodeID, &QueryOptions{})
	if err != nil {
		return nil, nil, err
	}

	if node.HTTPAddr == "" {
		return nil, nil, fmt.Errorf("http addr of the node where alloc %q is running is not advertised", alloc.ID)
	}
	u := &url.URL{
		Scheme: "http",
		Host:   node.HTTPAddr,
		Path:   fmt.Sprintf("/v1/client/fs/cat/%s", alloc.ID),
	}
	v := url.Values{}
	v.Set("path", path)
	u.RawQuery = v.Encode()
	req := &http.Request{
		Method: "GET",
		URL:    u,
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, nil, nil
}

func (a *AllocFS) getErrorMsg(resp *http.Response) error {
	if errMsg, err := ioutil.ReadAll(resp.Body); err == nil {
		return fmt.Errorf(string(errMsg))
	} else {
		return err
	}
}

// Stream streams the content of a file blocking on EOF.
// The parameters are:
// * path: path to file to stream.
// * offset: The offset to start streaming data at.
// * origin: Either "start" or "end" and defines from where the offset is applied.
//
// The return value is a channel that will emit StreamFrames as they are read.
func (a *AllocFS) Stream(alloc *Allocation, path, origin string, offset int64,
	cancel <-chan struct{}, q *QueryOptions) (<-chan *StreamFrame, *QueryMeta, error) {

	node, _, err := a.client.Nodes().Info(alloc.NodeID, q)
	if err != nil {
		return nil, nil, err
	}

	if node.HTTPAddr == "" {
		return nil, nil, fmt.Errorf("http addr of the node where alloc %q is running is not advertised", alloc.ID)
	}
	u := &url.URL{
		Scheme: "http",
		Host:   node.HTTPAddr,
		Path:   fmt.Sprintf("/v1/client/fs/stream/%s", alloc.ID),
	}
	v := url.Values{}
	v.Set("path", path)
	v.Set("origin", origin)
	v.Set("offset", strconv.FormatInt(offset, 10))
	u.RawQuery = v.Encode()
	req := &http.Request{
		Method: "GET",
		URL:    u,
		Cancel: cancel,
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// Create the output channel
	frames := make(chan *StreamFrame, 10)

	go func() {
		// Close the body
		defer resp.Body.Close()

		// Create a decoder
		dec := json.NewDecoder(resp.Body)

		for {
			// Check if we have been cancelled
			select {
			case <-cancel:
				return
			default:
			}

			// Decode the next frame
			var frame StreamFrame
			if err := dec.Decode(&frame); err != nil {
				close(frames)
				return
			}

			// Discard heartbeat frames
			if frame.IsHeartbeat() {
				continue
			}

			frames <- &frame
		}
	}()

	return frames, nil, nil
}

// FrameReader is used to convert a stream of frames into a read closer.
type FrameReader struct {
	frames   <-chan *StreamFrame
	cancelCh chan struct{}
	closed   bool

	frame       *StreamFrame
	frameOffset int

	// To handle printing the file events
	fileEventOffset int
	fileEvent       []byte

	byteOffset int
}

// NewFrameReader takes a channel of frames and returns a FrameReader which
// implements io.ReadCloser
func NewFrameReader(frames <-chan *StreamFrame, cancelCh chan struct{}) *FrameReader {
	return &FrameReader{
		frames:   frames,
		cancelCh: cancelCh,
	}
}

// Offset returns the offset into the stream.
func (f *FrameReader) Offset() int {
	return f.byteOffset
}

// Read reads the data of the incoming frames into the bytes buffer. Returns EOF
// when there are no more frames.
func (f *FrameReader) Read(p []byte) (n int, err error) {
	if f.frame == nil {
		frame, ok := <-f.frames
		if !ok {
			return 0, io.EOF
		}
		f.frame = frame

		// Store the total offset into the file
		f.byteOffset = int(f.frame.Offset)
	}

	if f.frame.FileEvent != "" && len(f.fileEvent) == 0 {
		f.fileEvent = []byte(fmt.Sprintf("\nnomad: %q\n", f.frame.FileEvent))
		f.fileEventOffset = 0
	}

	// If there is a file event we inject it into the read stream
	if l := len(f.fileEvent); l != 0 && l != f.fileEventOffset {
		n = copy(p, f.fileEvent[f.fileEventOffset:])
		f.fileEventOffset += n
		return n, nil
	}

	if len(f.fileEvent) == f.fileEventOffset {
		f.fileEvent = nil
		f.fileEventOffset = 0
	}

	// Copy the data out of the frame and update our offset
	n = copy(p, f.frame.Data[f.frameOffset:])
	f.frameOffset += n

	// Clear the frame and its offset once we have read everything
	if len(f.frame.Data) == f.frameOffset {
		f.frame = nil
		f.frameOffset = 0
	}

	return n, nil
}

// Close cancels the stream of frames
func (f *FrameReader) Close() error {
	if f.closed {
		return nil
	}

	close(f.cancelCh)
	f.closed = true
	return nil
}
