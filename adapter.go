package filesys

import (
	"context"
	"io"
	"time"
)

type File struct {
	ModTime time.Time
	Name    string
	Size    int64
	IsDir   bool
	Header  map[string]string
}

type Adapter interface {
	Delete(ctx context.Context, objects ...string) (err error)                                                       // 删除文件
	GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error)                         // 文件访问签名
	IsExist(ctx context.Context, object string) (err error)                                                          // 判断文件是否存在
	Lists(ctx context.Context, prefix string) (files []*File, err error)                                             // 文件前缀，列出文件
	Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) // 上传文件
	Download(ctx context.Context, object string) (body io.ReadCloser, err error)                                     // 下载文件
	GetInfo(ctx context.Context, object string) (info *File, err error)                                              // 获取指定文件信息
}
