package filesys

import (
	"context"
	"fmt"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type ConfigObs struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Endpoint  string `json:"endpoint" v:"required#Endpoint不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type ObsAdapter struct {
	config *ConfigObs
	client *obs.ObsClient
}

func NewAdapterObs(i interface{}) (Adapter, error) {
	cfg := (*ConfigObs)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}
	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}
	if cfg.Domain == "" {
		cfg.Domain = fmt.Sprintf("https://%v.%v", cfg.Bucket, cfg.Endpoint)
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/")

	o := &ObsAdapter{
		config: cfg,
	}

	var err error
	o.client, err = obs.New(cfg.AccessKey, cfg.SecretKey, cfg.Endpoint)
	return o, err
}

func (o *ObsAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = o.GetInfo(ctx, object)
	return
}

func (o *ObsAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	input := &obs.PutObjectInput{}
	input.Bucket = o.config.Bucket
	input.Key = objectRel(path)
	input.Metadata = make(map[string]string)
	input.Body = reader

	for _, header := range headers {
		for k, v := range header {
			switch strings.ToLower(k) {
			case "content-type":
				input.ContentType = v
			default:
				input.Metadata[k] = v
			}
		}
	}
	_, err = o.client.PutObject(input)
	return
}

func (o *ObsAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	if len(objects) <= 0 {
		return
	}
	var objs []obs.ObjectToDelete
	for _, object := range objects {
		objs = append(objs, obs.ObjectToDelete{
			Key: objectRel(object),
		})
	}
	input := &obs.DeleteObjectsInput{
		Bucket:  o.config.Bucket,
		Objects: objs,
	}
	_, err = o.client.DeleteObjects(input)
	return
}

func (o *ObsAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := o.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	if exp <= 0 {
		link = o.config.Domain + objectAbs(object)
		return
	}
	input := &obs.CreateSignedUrlInput{
		Method:  http.MethodGet,
		Bucket:  o.config.Bucket,
		Key:     objectRel(object),
		Expires: int(exp),
	}
	output := &obs.CreateSignedUrlOutput{}
	output, err = o.client.CreateSignedUrl(input)
	if err != nil {
		return
	}
	link = output.SignedUrl
	if !strings.HasPrefix(link, o.config.Domain) {
		if u, errU := url.Parse(link); errU == nil {
			link = o.config.Domain + u.RequestURI()
		}
	}
	return
}

func (o *ObsAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	input := &obs.GetObjectInput{}
	input.Key = objectRel(object)
	input.Bucket = o.config.Bucket

	output, err := o.client.GetObject(input)
	if err != nil {
		return
	}
	body = output.Body
	return
}

func (o *ObsAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	input := &obs.GetObjectMetadataInput{
		Bucket: o.config.Bucket,
		Key:    objectRel(object),
	}
	output := &obs.GetObjectMetadataOutput{}
	output, err = o.client.GetObjectMetadata(input)
	if err != nil {
		return
	}
	info = &File{
		Name:    objectRel(object),
		Size:    output.ContentLength,
		IsDir:   output.ContentLength == 0,
		ModTime: output.LastModified,
	}
	return
}

func (o *ObsAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	prefix = objectRel(prefix)
	input := &obs.ListObjectsInput{}
	input.Prefix = prefix
	input.Bucket = o.config.Bucket
	output := &obs.ListObjectsOutput{}
	output, err = o.client.ListObjects(input)
	if err != nil {
		return
	}

	for _, item := range output.Contents {
		files = append(files, &File{
			ModTime: item.LastModified,
			Name:    objectRel(item.Key),
			Size:    item.Size,
			IsDir:   item.Size == 0,
		})
	}

	return
}
