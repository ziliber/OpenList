package stream

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/net"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/pool"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/rclone/rclone/lib/mmap"
	log "github.com/sirupsen/logrus"
)

type RangeReaderFunc func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error)

func (f RangeReaderFunc) RangeRead(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
	return f(ctx, httpRange)
}

func GetRangeReaderFromLink(size int64, link *model.Link) (model.RangeReaderIF, error) {
	if link.Concurrency > 0 || link.PartSize > 0 {
		down := net.NewDownloader(func(d *net.Downloader) {
			d.Concurrency = link.Concurrency
			d.PartSize = link.PartSize
		})
		var rangeReader RangeReaderFunc = func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
			var req *net.HttpRequestParams
			if link.RangeReader != nil {
				req = &net.HttpRequestParams{
					Range: httpRange,
					Size:  size,
				}
			} else {
				requestHeader, _ := ctx.Value(conf.RequestHeaderKey).(http.Header)
				header := net.ProcessHeader(requestHeader, link.Header)
				req = &net.HttpRequestParams{
					Range:     httpRange,
					Size:      size,
					URL:       link.URL,
					HeaderRef: header,
				}
			}
			return down.Download(ctx, req)
		}
		if link.RangeReader != nil {
			down.HttpClient = net.GetRangeReaderHttpRequestFunc(link.RangeReader)
			return rangeReader, nil
		}
		return RateLimitRangeReaderFunc(rangeReader), nil
	}

	if link.RangeReader != nil {
		return link.RangeReader, nil
	}

	if len(link.URL) == 0 {
		return nil, errors.New("invalid link: must have at least one of URL or RangeReader")
	}
	rangeReader := func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
		if httpRange.Length < 0 || httpRange.Start+httpRange.Length > size {
			httpRange.Length = size - httpRange.Start
		}
		requestHeader, _ := ctx.Value(conf.RequestHeaderKey).(http.Header)
		header := net.ProcessHeader(requestHeader, link.Header)
		header = http_range.ApplyRangeToHttpHeader(httpRange, header)

		response, err := net.RequestHttp(ctx, "GET", header, link.URL)
		if err != nil {
			if _, ok := errs.UnwrapOrSelf(err).(net.HttpStatusCodeError); ok {
				return nil, err
			}
			return nil, fmt.Errorf("http request failure, err:%w", err)
		}
		if httpRange.Start == 0 && (httpRange.Length == -1 || httpRange.Length == size) || response.StatusCode == http.StatusPartialContent ||
			checkContentRange(&response.Header, httpRange.Start) {
			return response.Body, nil
		} else if response.StatusCode == http.StatusOK {
			log.Warnf("remote http server not supporting range request, expect low perfromace!")
			readCloser, err := net.GetRangedHttpReader(response.Body, httpRange.Start, httpRange.Length)
			if err != nil {
				return nil, err
			}
			return readCloser, nil
		}
		return response.Body, nil
	}
	return RateLimitRangeReaderFunc(rangeReader), nil
}

// RangeReaderIF.RangeRead返回的io.ReadCloser保留file的签名。
func GetRangeReaderFromMFile(size int64, file model.File) model.RangeReaderIF {
	return &model.FileRangeReader{
		RangeReaderIF: RangeReaderFunc(func(ctx context.Context, httpRange http_range.Range) (io.ReadCloser, error) {
			length := httpRange.Length
			if length < 0 || httpRange.Start+length > size {
				length = size - httpRange.Start
			}
			return &model.FileCloser{File: io.NewSectionReader(file, httpRange.Start, length)}, nil
		}),
	}
}

// 139 cloud does not properly return 206 http status code, add a hack here
func checkContentRange(header *http.Header, offset int64) bool {
	start, _, err := http_range.ParseContentRange(header.Get("Content-Range"))
	if err != nil {
		log.Warnf("exception trying to parse Content-Range, will ignore,err=%s", err)
	}
	if start == offset {
		return true
	}
	return false
}

type ReaderWithCtx struct {
	io.Reader
	Ctx context.Context
}

func (r *ReaderWithCtx) Read(p []byte) (n int, err error) {
	if utils.IsCanceled(r.Ctx) {
		return 0, r.Ctx.Err()
	}
	return r.Reader.Read(p)
}

func (r *ReaderWithCtx) Close() error {
	if c, ok := r.Reader.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func CacheFullAndHash(stream model.FileStreamer, up *model.UpdateProgress, hashType *utils.HashType, hashParams ...any) (model.File, string, error) {
	h := hashType.NewFunc(hashParams...)
	tmpF, err := stream.CacheFullAndWriter(up, h)
	if err != nil {
		return nil, "", err
	}
	return tmpF, hex.EncodeToString(h.Sum(nil)), nil
}

type StreamSectionReaderIF interface {
	// 线程不安全
	GetSectionReader(off, length int64) (io.ReadSeeker, error)
	FreeSectionReader(sr io.ReadSeeker)
	// 线程不安全
	DiscardSection(off int64, length int64) error
}

func NewStreamSectionReader(file model.FileStreamer, maxBufferSize int, up *model.UpdateProgress) (StreamSectionReaderIF, error) {
	if file.GetFile() != nil {
		return &cachedSectionReader{file.GetFile()}, nil
	}

	maxBufferSize = min(maxBufferSize, int(file.GetSize()))
	if maxBufferSize > conf.MaxBufferLimit {
		f, err := os.CreateTemp(conf.Conf.TempDir, "file-*")
		if err != nil {
			return nil, err
		}

		if f.Truncate((file.GetSize()+int64(maxBufferSize-1))/int64(maxBufferSize)*int64(maxBufferSize)) != nil {
			// fallback to full cache
			_, _ = f.Close(), os.Remove(f.Name())
			cache, err := file.CacheFullAndWriter(up, nil)
			if err != nil {
				return nil, err
			}
			return &cachedSectionReader{cache}, nil
		}

		ss := &fileSectionReader{Reader: file, temp: f}
		ss.bufPool = &pool.Pool[*offsetWriterWithBase]{
			New: func() *offsetWriterWithBase {
				base := ss.fileOff
				ss.fileOff += int64(maxBufferSize)
				return &offsetWriterWithBase{io.NewOffsetWriter(ss.temp, base), base}
			},
		}
		file.Add(utils.CloseFunc(func() error {
			ss.bufPool.Reset()
			return errors.Join(ss.temp.Close(), os.Remove(ss.temp.Name()))
		}))
		return ss, nil
	}

	ss := &directSectionReader{file: file}
	if conf.MmapThreshold > 0 && maxBufferSize >= conf.MmapThreshold {
		ss.bufPool = &pool.Pool[[]byte]{
			New: func() []byte {
				buf, err := mmap.Alloc(maxBufferSize)
				if err == nil {
					ss.file.Add(utils.CloseFunc(func() error {
						return mmap.Free(buf)
					}))
				} else {
					buf = make([]byte, maxBufferSize)
				}
				return buf
			},
		}
	} else {
		ss.bufPool = &pool.Pool[[]byte]{
			New: func() []byte {
				return make([]byte, maxBufferSize)
			},
		}
	}

	file.Add(utils.CloseFunc(func() error {
		ss.bufPool.Reset()
		return nil
	}))
	return ss, nil
}

type cachedSectionReader struct {
	cache io.ReaderAt
}

func (*cachedSectionReader) DiscardSection(off int64, length int64) error {
	return nil
}
func (s *cachedSectionReader) GetSectionReader(off, length int64) (io.ReadSeeker, error) {
	return io.NewSectionReader(s.cache, off, length), nil
}
func (*cachedSectionReader) FreeSectionReader(sr io.ReadSeeker) {}

type fileSectionReader struct {
	io.Reader
	off     int64
	temp    *os.File
	fileOff int64
	bufPool *pool.Pool[*offsetWriterWithBase]
}

type offsetWriterWithBase struct {
	*io.OffsetWriter
	base int64
}

// 线程不安全
func (ss *fileSectionReader) DiscardSection(off int64, length int64) error {
	if off != ss.off {
		return fmt.Errorf("stream not cached: request offset %d != current offset %d", off, ss.off)
	}
	_, err := utils.CopyWithBufferN(io.Discard, ss.Reader, length)
	if err != nil {
		return fmt.Errorf("failed to skip data: (expect =%d) %w", length, err)
	}
	ss.off += length
	return nil
}

type fileBufferSectionReader struct {
	io.ReadSeeker
	fileBuf *offsetWriterWithBase
}

func (ss *fileSectionReader) GetSectionReader(off, length int64) (io.ReadSeeker, error) {
	if off != ss.off {
		return nil, fmt.Errorf("stream not cached: request offset %d != current offset %d", off, ss.off)
	}
	fileBuf := ss.bufPool.Get()
	_, _ = fileBuf.Seek(0, io.SeekStart)
	n, err := utils.CopyWithBufferN(fileBuf, ss.Reader, length)
	if err != nil {
		return nil, fmt.Errorf("failed to read all data: (expect =%d, actual =%d) %w", length, n, err)
	}
	ss.off += length
	return &fileBufferSectionReader{io.NewSectionReader(ss.temp, fileBuf.base, length), fileBuf}, nil
}

func (ss *fileSectionReader) FreeSectionReader(rs io.ReadSeeker) {
	if sr, ok := rs.(*fileBufferSectionReader); ok {
		ss.bufPool.Put(sr.fileBuf)
		sr.fileBuf = nil
		sr.ReadSeeker = nil
	}
}

type directSectionReader struct {
	file    model.FileStreamer
	off     int64
	bufPool *pool.Pool[[]byte]
}

// 线程不安全
func (ss *directSectionReader) DiscardSection(off int64, length int64) error {
	if off != ss.off {
		return fmt.Errorf("stream not cached: request offset %d != current offset %d", off, ss.off)
	}
	_, err := utils.CopyWithBufferN(io.Discard, ss.file, length)
	if err != nil {
		return fmt.Errorf("failed to skip data: (expect =%d) %w", length, err)
	}
	ss.off += length
	return nil
}

type bufferSectionReader struct {
	io.ReadSeeker
	buf []byte
}

// 线程不安全
func (ss *directSectionReader) GetSectionReader(off, length int64) (io.ReadSeeker, error) {
	if off != ss.off {
		return nil, fmt.Errorf("stream not cached: request offset %d != current offset %d", off, ss.off)
	}
	tempBuf := ss.bufPool.Get()
	buf := tempBuf[:length]
	n, err := io.ReadFull(ss.file, buf)
	if int64(n) != length {
		return nil, fmt.Errorf("failed to read all data: (expect =%d, actual =%d) %w", length, n, err)
	}
	ss.off += int64(n)
	return &bufferSectionReader{bytes.NewReader(buf), buf}, nil
}
func (ss *directSectionReader) FreeSectionReader(rs io.ReadSeeker) {
	if sr, ok := rs.(*bufferSectionReader); ok {
		ss.bufPool.Put(sr.buf[0:cap(sr.buf)])
		sr.buf = nil
		sr.ReadSeeker = nil
	}
}
