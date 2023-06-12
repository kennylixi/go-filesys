package filesys

import (
	"bytes"
	"context"
	"errors"
	"io"
)

var (
	ParamsErr      = errors.New("文件存储驱动配置参数错误")
	NotExitsCfgErr = errors.New("文件存储驱动配置不存在")
)

type Store struct {
	localAdapter
}

type localAdapter = Adapter

func NewWithAdapter(adapter Adapter) *Store {
	return &Store{
		localAdapter: adapter,
	}
}

func (c *Store) SetAdapter(adapter Adapter) {
	c.localAdapter = adapter
}

func (c *Store) GetAdapter() Adapter {
	return c.localAdapter
}

// Delete 删除文件
func (c *Store) Delete(ctx context.Context, object string) (err error) {
	return c.localAdapter.Delete(ctx, object)
}

// Deletes 删除文件
func (c *Store) Deletes(ctx context.Context, objects []string) (err error) {
	return c.localAdapter.Delete(ctx, objects...)
}

// GetSignURL 文件访问签名
func (c *Store) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	if len(expire) > 0 {
		c.localAdapter.GetSignURL(ctx, object, expire...)
	}
	return c.localAdapter.GetSignURL(ctx, object)
}

// IsExist 判断文件是否存在
func (c *Store) IsExist(ctx context.Context, object string) (err error) {
	return c.localAdapter.IsExist(ctx, object)
}

// Lists 文件前缀，列出文件
func (c *Store) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	return c.localAdapter.Lists(ctx, prefix)
}

// Upload 上传文件
func (c *Store) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	if len(headers) > 0 {
		return c.localAdapter.Upload(ctx, path, reader, size, headers...)
	}
	return c.localAdapter.Upload(ctx, path, reader, size)
}

// Download 下载文件
func (c *Store) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	return c.localAdapter.Download(ctx, object)
}

// GetInfo 获取指定文件信息
func (c *Store) GetInfo(ctx context.Context, object string) (info *File, err error) {
	return c.localAdapter.GetInfo(ctx, object)
}

func (c *Store) PingTest(ctx context.Context) (err error) {
	saveFile := "test-file.txt"
	text := "test-file"

	if err = c.Upload(ctx, saveFile, bytes.NewReader([]byte(text)), int64(len(text))); err != nil {
		return
	}
	if err = c.IsExist(ctx, saveFile); err != nil {
		return
	}
	if err = c.Delete(ctx, saveFile); err != nil {
		return
	}
	return
}
