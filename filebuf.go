// Copyright 2020 Giulio Iotti. All rights reserved.
// This package is provided without warranty; any use is granted by the author.

// Package filebuf implements a buffer similar to bytes.Buffer backed by
// either a buffer or a temporary file.
//
// Filebuf switches from an in-memory buffer to a temporary file after
// writing a configurable number of bytes.
package filebuf

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"syscall"
)

// Filebuf provides a scratch space backed by memory or temporary
// file depending on size of the written data.
//
// Intended usage is for writing and then reading back written data.
// Use Clone() for multiple concurrent readers.
//
// Mixing of concurrent reads and writes is not supported.
type Filebuf struct {
	// Maximum memory size to use before switching to disk files.
	// Default means using memory only.
	MaxBufSize int
	// Ignore errors when removing the temporary file. Ignoring this
	// error results in silent creation of temporary files on disk.
	IgnoreDeleteErr bool
	// Directory path to use to store temporary files. Empty means system default.
	TempDir string
	// Pattern to use to name temporary files. Same as ioutil.TempFile.
	TempFilePattern string

	buf  []byte
	file *os.File
	off  int64 // offset reading
}

// New returns a Filebuf ready to be used. Public parameters can still be
// changed before using the buffer.
//
// Returned Filebuf will switch to a temporary file after writing size bytes to memory.
func New(size int) *Filebuf {
	return &Filebuf{
		MaxBufSize: size,
	}
}

func (f *Filebuf) moveToFile() error {
	var err error
	f.file, err = ioutil.TempFile(f.TempDir, f.TempFilePattern)
	if err != nil {
		return fmt.Errorf("cannot open backing temporary file: %w", err)
	}
	if err = os.Remove(f.file.Name()); err != nil && !f.IgnoreDeleteErr {
		return fmt.Errorf("cannot delete backing temporary file: %w", err)
	}
	if _, err = f.file.Write(f.buf); err != nil {
		return fmt.Errorf("cannot copy to backing temporary file: %w", err)
	}
	return nil
}

func (f *Filebuf) appendBuffer(p []byte) {
	if f.buf == nil {
		f.buf = make([]byte, 0, f.MaxBufSize)
	}
	// append is guaranteed to be called within cap boundaries
	l := len(f.buf)
	f.buf = f.buf[:l+len(p)]
	copy(f.buf[l:], p)
}

func (f *Filebuf) copyBuffer(p []byte) int {
	end := int(f.off) + len(p)
	if end > len(f.buf) {
		end = len(f.buf)
	}
	copy(p, f.buf[f.off:end])
	return end - int(f.off)
}

// Write writes to the backing buffer or disk file and returns the
// number of written bytes or an error.
func (f *Filebuf) Write(p []byte) (n int, err error) {
	if f.file != nil {
		return f.file.Write(p)
	}
	if f.MaxBufSize > 0 && len(f.buf)+len(p) > f.MaxBufSize {
		if err := f.moveToFile(); err != nil {
			return 0, err
		}
		f.buf = nil
		return f.file.Write(p)
	}
	f.appendBuffer(p)
	return len(p), nil
}

// Read reads in slice p and returns the number of bytes read or and error.
func (f *Filebuf) Read(p []byte) (n int, err error) {
	if f.file != nil {
		n, err := f.file.ReadAt(p, f.off)
		f.off += int64(n)
		return n, err
	}
	if int(f.off) >= len(f.buf) {
		return 0, io.EOF
	}
	n = f.copyBuffer(p)
	f.off += int64(n)
	return n, nil
}

// ReadAt reads up to len(p) bytes at position pos. ReadAt is not safe for concurrent usage.
func (f *Filebuf) ReadAt(p []byte, pos int64) (n int, err error) {
	off := f.off
	f.off = pos
	n, err = f.Read(p)
	f.off = off
	return n, err
}

// ReadFrom reads r in full into the backing buffer or file. Returns the
// number of read bytes or an error.
func (f *Filebuf) ReadFrom(r io.Reader) (n int64, err error) {
	if f.file != nil {
		return io.Copy(f.file, r)
	}
	if f.MaxBufSize == 0 {
		b := new(bytes.Buffer)
		n, err = io.Copy(b, r)
		f.buf = append(f.buf, b.Bytes()...)
		return n, err
	}
	if f.buf == nil {
		f.buf = make([]byte, 0, f.MaxBufSize)
	}
	var tot int64
	// read until limit; if limit is hit, switch to file to continue copying
	for {
		m, err := r.Read(f.buf[len(f.buf) : cap(f.buf)-len(f.buf)])
		f.buf = f.buf[:len(f.buf)+m]
		tot += int64(m)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return tot, err
		}
		if len(f.buf) == cap(f.buf) {
			break
		}
	}
	m := len(f.buf)
	if err := f.moveToFile(); err != nil {
		return 0, err
	}
	f.buf = nil
	n, err = io.Copy(f.file, r)
	return n + int64(m), err
}

// WriteTo writes the full contents of the buffer into w. Returns the
// number of written bytes or an error.
func (f *Filebuf) WriteTo(w io.Writer) (n int64, err error) {
	if f.file != nil {
		if _, err := f.file.Seek(f.off, 0); err != nil {
			return 0, fmt.Errorf("cannot seek in backing file: %w", err)
		}
		return io.Copy(w, f.file)
	}
	m, err := w.Write(f.buf[f.off:])
	return int64(m), err
}

// Rewind resets the next read to the beginning of the buffer.
func (f *Filebuf) Rewind() error {
	f.off = 0
	if f.file != nil {
		if _, err := f.file.Seek(0, 0); err != nil {
			return err
		}
	}
	return nil
}

// Clone creates a new Filebuf sharing the same backing memory and a duplicated file handle of
// the same temporary backing file, if used.
//
// Clone is intended to provide multiple readers after writing has finished.
//
// Writing to either Filebuf after clone is not supported.
//
// All cloned Filebufs need to be closed after use.
func (f *Filebuf) Clone() (*Filebuf, error) {
	fb := *f
	if fb.file != nil {
		fd, err := syscall.Dup(int(f.file.Fd()))
		if err != nil {
			return nil, fmt.Errorf("cannot duplicate handle to backing file: %w", err)
		}
		name := f.file.Name()
		fb.file = os.NewFile(uintptr(fd), name)
		if fb.file == nil {
			return nil, fmt.Errorf("could not create new file from descriptor %d", fd)
		}
	}
	return &fb, nil
}

// Close closes the underlying buffer or file. Closing might return an error.
func (f *Filebuf) Close() error {
	if f.file != nil {
		return f.file.Close()
	}
	f.buf = nil
	return nil
}
