package filesys

import (
	"context"
	"errors"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go"
)

type ConfigMinio struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Endpoint  string `json:"endpoint" v:"required#Endpoint不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type MinIoAdapter struct {
	config *ConfigMinio
	client *minio.Client
}

func NewAdapterMinio(i interface{}) (Adapter, error) {
	cfg := (*ConfigMinio)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}
	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}

	if cfg.Domain == "" {
		cfg.Domain = "http://" + cfg.Endpoint
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/")

	m := &MinIoAdapter{
		config: cfg,
	}
	var err error
	m.client, err = minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, false)
	return m, err
}

func (m *MinIoAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = m.GetInfo(ctx, object)
	return
}

func (m *MinIoAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	opts := minio.PutObjectOptions{
		UserMetadata: make(map[string]string),
	}

	for _, header := range headers {
		for k, v := range header {
			switch strings.ToLower(k) {
			case "content-disposition":
				opts.ContentDisposition = v
			case "content-encoding":
				opts.ContentEncoding = v
			case "content-type":
				opts.ContentType = v
			default:
				opts.UserMetadata[k] = v
			}
		}
	}

	_, err = m.client.PutObject(m.config.Bucket, objectRel(path), reader, size, opts)
	return
}

func (m *MinIoAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	if len(objects) == 0 {
		return
	}

	var errs []string

	objectsChan := make(chan string)
	go func() {
		defer close(objectsChan)
		for _, object := range objects {
			objectsChan <- objectRel(object)
		}
	}()
	for errRm := range m.client.RemoveObjects(m.config.Bucket, objectsChan) {
		if errRm.Err != nil {
			errs = append(errs, errRm.Err.Error())
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
	}
	return
}

func (m *MinIoAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := m.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	if exp <= 0 {
		link = m.config.Domain + objectAbs(object)
		return
	}
	if exp > sevenDays {
		exp = sevenDays
	}
	u := &url.URL{}
	u, err = m.client.PresignedGetObject(m.config.Bucket, objectRel(object), time.Duration(m.config.Expire)*time.Second, nil)
	if err != nil {
		return
	}
	link = u.String()
	if !strings.HasPrefix(link, m.config.Domain) {
		link = m.config.Domain + u.RequestURI()
	}
	return
}

func (m *MinIoAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	obj := &minio.Object{}
	obj, err = m.client.GetObject(m.config.Bucket, objectRel(object), minio.GetObjectOptions{})
	if err != nil {
		return
	}
	body = obj
	return
}

func (m *MinIoAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	var objInfo minio.ObjectInfo
	opts := minio.StatObjectOptions{}
	object = objectRel(object)
	objInfo, err = m.client.StatObject(m.config.Bucket, object, opts)
	if err != nil {
		return
	}
	info = &File{
		ModTime: objInfo.LastModified,
		Name:    object,
		Size:    objInfo.Size,
		IsDir:   objInfo.Size == 0,
		Header:  make(map[string]string),
	}
	for k, _ := range objInfo.Metadata {
		info.Header[k] = objInfo.Metadata.Get(k)
	}
	return
}

func (m *MinIoAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	prefix = objectRel(prefix)
	doneCh := make(chan struct{})
	defer close(doneCh)
	objects := m.client.ListObjectsV2(m.config.Bucket, prefix, true, doneCh)
	for object := range objects {
		header := make(map[string]string)
		file := &File{
			ModTime: object.LastModified,
			Size:    object.Size,
			IsDir:   object.Size == 0,
			Name:    objectRel(object.Key),
		}
		for k, _ := range object.Metadata {
			header[k] = object.Metadata.Get(k)
		}
		files = append(files, file)
	}
	return
}
