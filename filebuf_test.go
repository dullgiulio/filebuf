package filebuf

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"io"
	"testing"
)

type castFn func(fb *Filebuf) (io.Reader, io.Writer)

func testChecksum(t *testing.T, cast castFn, maxSize int, totSize int64, expectFile bool) {
	fb := &Filebuf{MaxBufSize: maxSize}
	fbr, fbw := cast(fb)
	lr := &io.LimitedReader{R: rand.Reader, N: totSize}

	h1 := sha1.New()
	tr := io.TeeReader(lr, h1)
	n1, err := io.Copy(fbw, tr)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if expectFile && fb.file == nil {
		t.Fatalf("expected usage of file in big read")
	}

	h2 := sha1.New()
	n2, err := io.Copy(h2, fbr)
	if err != nil {
		t.Fatalf("expected copy success of file to hash, got %v", err)
	}
	if n1 != n2 {
		t.Fatalf("wrote %d bytes, but read %d", n1, n2)
	}
	s1 := h1.Sum(nil)
	s2 := h2.Sum(nil)
	if bytes.Compare(s1, s2) != 0 {
		t.Fatalf("checksums mismatch: %x != %x", s1, s2)
	}

	if err := fb.Close(); err != nil {
		t.Fatalf("unexpected error closing filebuf: %v", err)
	}
}

func TestCopyChecksum(t *testing.T) {
	sizes := []struct {
		max  int
		tot  int64
		file bool
	}{
		{1 << 8, 1 << 12, true},
		{1 << 8, 1 << 8, false},
		{1 << 12, 1 << 8, false},
	}
	// test with and without ReadFrom and WriteTo
	casts := []castFn{
		func(fb *Filebuf) (io.Reader, io.Writer) { return fb, fb },
		func(fb *Filebuf) (io.Reader, io.Writer) { return struct{ io.Reader }{fb}, fb },
		func(fb *Filebuf) (io.Reader, io.Writer) { return fb, struct{ io.Writer }{fb} },
		func(fb *Filebuf) (io.Reader, io.Writer) { return struct{ io.Reader }{fb}, struct{ io.Writer }{fb} },
		func(fb *Filebuf) (io.Reader, io.Writer) {
			return struct {
				io.Reader
				io.ReaderFrom
			}{fb, fb}, fb
		},
		func(fb *Filebuf) (io.Reader, io.Writer) {
			return fb, struct {
				io.Writer
				io.WriterTo
			}{fb, fb}
		},
	}
	for _, size := range sizes {
		for _, cast := range casts {
			testChecksum(t, cast, size.max, size.tot, size.file)
		}
	}
}
