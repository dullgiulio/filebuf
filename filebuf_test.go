package filebuf

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"io"
	"io/ioutil"
	"testing"
)

type castFn func(fb *Filebuf) (io.Reader, io.Writer)

func copySum(src io.Reader, w io.Writer) ([]byte, int64, error) {
	h := sha1.New()
	tr := io.TeeReader(src, h)
	n, err := io.Copy(w, tr)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
}

func readSum(r io.Reader) ([]byte, int64, error) {
	h := sha1.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return nil, 0, err
	}
	return h.Sum(nil), n, nil
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
			fb := &Filebuf{MaxBufSize: size.max}
			src := &io.LimitedReader{R: rand.Reader, N: size.tot}
			r, w := cast(fb)

			s1, n1, err := copySum(src, w)
			if err != nil {
				t.Fatalf("unexpected error filling Filebuf: %v", err)
			}

			if size.file && fb.file == nil {
				t.Fatalf("expected usage of file in big read")
			}

			s2, n2, err := readSum(r)
			if err != nil {
				t.Fatalf("unexpected error readinf Filebuf: %v", err)
			}

			if n1 != n2 {
				t.Fatalf("wrote %d bytes, but read %d", n1, n2)
			}
			if bytes.Compare(s1, s2) != 0 {
				t.Fatalf("checksums mismatch: %x != %x", s1, s2)
			}
			if err := fb.Close(); err != nil {
				t.Fatalf("unexpected error closing filebuf: %v", err)
			}
		}
	}
}

func TestClone(t *testing.T) {
	fb := &Filebuf{MaxBufSize: 1 << 8}
	src := &io.LimitedReader{R: rand.Reader, N: 1 << 14}

	s1, n1, err := copySum(src, fb)
	if err != nil {
		t.Fatalf("unexpected error filling Filebuf: %v", err)
	}

	if fb.file == nil {
		t.Fatalf("expected usage of file in big read")
	}

	fbr, err := fb.Clone()
	if err != nil {
		t.Fatalf("unexpected error cloning Filebuf: %v", err)
	}

	// go to end of fb, should not change offset of clone fbr
	_, err = io.Copy(ioutil.Discard, fb)
	if err != nil {
		t.Fatalf("unexpected error consuming original Filebuf: %v", err)
	}
	if err := fb.Close(); err != nil {
		t.Fatalf("unexpected error closing filebuf: %v", err)
	}

	s2, n2, err := readSum(fbr)
	if err != nil {
		t.Fatalf("unexpected error reading Filebuf: %v", err)
	}

	if n1 != n2 {
		t.Fatalf("wrote %d bytes, but read %d", n1, n2)
	}
	if bytes.Compare(s1, s2) != 0 {
		t.Fatalf("checksums mismatch: %x != %x", s1, s2)
	}

	if err := fbr.Rewind(); err != nil {
		t.Fatalf("unexpected error when rewinding: %v", err)
	}
	s3, n3, err := readSum(fbr)
	if err != nil {
		t.Fatalf("unexpected error reading Filebuf: %v", err)
	}

	if n3 != n2 {
		t.Fatalf("read %d bytes, but first read %d", n3, n2)
	}
	if bytes.Compare(s3, s2) != 0 {
		t.Fatalf("checksums mismatch: %x != %x", s3, s2)
	}
	if err := fbr.Close(); err != nil {
		t.Fatalf("unexpected error closing filebuf: %v", err)
	}
}
