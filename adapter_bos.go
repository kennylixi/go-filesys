package filesys

import (
	"context"
	"fmt"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ConfigBos struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Endpoint  string `json:"endpoint" v:"required#Endpoint不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type BosAdapter struct {
	config *ConfigBos
	client *bos.Client
}

func NewAdapterBos(i interface{}) (Adapter, error) {
	cfg := (*ConfigBos)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}
	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}

	if cfg.Domain == "" {
		cfg.Domain = "https://" + cfg.Bucket + "." + cfg.Endpoint
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/")

	b := &BosAdapter{
		config: cfg,
	}
	var err error
	b.client, err = bos.NewClient(cfg.AccessKey, cfg.SecretKey, cfg.Endpoint)
	return b, err
}

func (b *BosAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = b.GetInfo(ctx, object)
	return
}

func (b *BosAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	var args = &api.PutObjectArgs{
		UserMeta: make(map[string]string),
	}
	for _, header := range headers {
		for k, v := range header {
			switch strings.ToLower(k) {
			case "content-disposition":
				args.ContentDisposition = v
			case "content-type":
				args.ContentType = v
			//case "content-encoding":
			//	args.ContentEncoding = v
			default:
				args.UserMeta[k] = v
			}
		}
	}
	_, err = b.client.PutObjectFromStream(b.config.Bucket, objectRel(path), reader, args)
	return
}

func (b *BosAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	if len(objects) == 0 {
		return
	}
	for idx, object := range objects {
		objects[idx] = objectRel(object)
	}
	res, _ := b.client.DeleteMultipleObjectsFromKeyList(b.config.Bucket, objects)
	if res != nil && len(res.Errors) > 0 {
		err = fmt.Errorf("%+v", res)
	}
	return
}

func (b *BosAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := b.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	if exp <= 0 {
		link = b.config.Domain + objectAbs(object)
	} else {
		link = b.client.BasicGeneratePresignedUrl(b.config.Bucket, objectRel(object), int(exp))
		if !strings.HasPrefix(link, b.config.Domain) {
			if u, errU := url.Parse(link); errU == nil {
				link = b.config.Domain + u.RequestURI()
			}
		}
	}
	return
}

func (b *BosAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	result, err := b.client.GetObject(b.config.Bucket, objectRel(object), nil)
	if err != nil {
		return
	}
	body = result.Body
	return
}

func (b *BosAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	var resp *api.GetObjectMetaResult
	resp, err = b.client.GetObjectMeta(b.config.Bucket, objectRel(object))
	if err != nil {
		return
	}
	info = &File{
		Name:   objectRel(object),
		Size:   resp.ContentLength,
		IsDir:  resp.ContentLength == 0,
		Header: resp.UserMeta,
	}
	info.ModTime, _ = time.Parse(http.TimeFormat, resp.LastModified)
	return
}

func (b *BosAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	var resp *api.ListObjectsResult
	args := &api.ListObjectsArgs{
		Prefix:  objectRel(prefix),
		MaxKeys: 1000,
	}
	resp, err = b.client.ListObjects(b.config.Bucket, args)
	if err != nil {
		return
	}

	for _, object := range resp.Contents {
		file := &File{
			Size:  int64(object.Size),
			Name:  objectRel(object.Key),
			IsDir: object.Size == 0,
		}
		file.ModTime, _ = time.Parse(http.TimeFormat, object.LastModified)
		files = append(files, file)
	}
	return
}
