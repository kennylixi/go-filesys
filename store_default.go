package filesys

import (
	"context"
	"github.com/gogf/gf/v2/errors/gerror"
	"io"
)

// RegistAdapter 注册适配器
func RegistAdapter(adapterType string, adapter NewAdapter) {
	adapters[adapterType] = adapter
}

// NewStore 实例化一个新的动态存储器
func NewStore(adapterType string, cfg interface{}) (*Store, error) {
	adapterFun, ok := adapters[adapterType]
	if !ok {
		return nil, gerror.Newf("不存在[%s]类型的适配器", adapterType)
	}
	adapter, err := adapterFun(cfg)
	if err != nil {
		return nil, err
	}
	return NewWithAdapter(adapter), nil
}

// Delete 删除文件
func Delete(ctx context.Context, object string) (err error) {
	return defaultStore.Delete(ctx, object)
}

// Deletes 删除文件
func Deletes(ctx context.Context, objects []string) (err error) {
	return defaultStore.Deletes(ctx, objects)
}

// GetSignURL 文件访问签名
func GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	if len(expire) > 0 {
		defaultStore.GetSignURL(ctx, object, expire...)
	}
	return defaultStore.GetSignURL(ctx, object)
}

// IsExist 判断文件是否存在
func IsExist(ctx context.Context, object string) (err error) {
	return defaultStore.IsExist(ctx, object)
}

// Lists 文件前缀，列出文件
func Lists(ctx context.Context, prefix string) (files []*File, err error) {
	return defaultStore.Lists(ctx, prefix)
}

// Upload 上传文件
func Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	if len(headers) > 0 {
		return defaultStore.Upload(ctx, path, reader, size, headers...)
	}
	return defaultStore.Upload(ctx, path, reader, size)
}

// Download 下载文件
func Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	return defaultStore.Download(ctx, object)
}

// GetInfo 获取指定文件信息
func GetInfo(ctx context.Context, object string) (info *File, err error) {
	return defaultStore.GetInfo(ctx, object)
}

func PingTest(ctx context.Context) (err error) {
	return defaultStore.PingTest(ctx)
}
