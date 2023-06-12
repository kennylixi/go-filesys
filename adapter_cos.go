package filesys

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
)

type ConfigCos struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Region    string `json:"region" v:"required#Region不能为空"`
	AppId     string `json:"appId" v:"required#AppId不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type CosAdapter struct {
	config *ConfigCos
	client *cos.Client
}

func NewAdapterCos(i interface{}) (Adapter, error) {
	cfg := (*ConfigCos)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}

	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}

	u, err := url.Parse(fmt.Sprintf("https://%v-%v.cos.%v.myqcloud.com", cfg.Bucket, cfg.AppId, cfg.Region))
	if err != nil {
		return nil, err
	}
	if cfg.Domain == "" {
		cfg.Domain = u.String()
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/ ")

	c := &CosAdapter{
		config: cfg,
	}

	c.client = cos.NewClient(
		&cos.BaseURL{BucketURL: u},
		&http.Client{
			Timeout: 1800 * time.Second,
			Transport: &cos.AuthorizationTransport{
				SecretID:  cfg.AccessKey,
				SecretKey: cfg.SecretKey,
			},
		})
	return c, nil
}

func (c *CosAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = c.GetInfo(ctx, object)
	return
}

func (c *CosAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	objHeader := &cos.ObjectPutHeaderOptions{}
	for _, header := range headers {
		for k, v := range header {
			switch strings.ToLower(k) {
			case "content-encoding":
				objHeader.ContentEncoding = v
			case "content-type":
				objHeader.ContentType = v
			case "content-disposition":
				objHeader.ContentDisposition = v
			}
		}
	}
	opt := &cos.ObjectPutOptions{ObjectPutHeaderOptions: objHeader}
	_, err = c.client.Object.Put(context.Background(), objectRel(path), reader, opt)
	return
}

func (c *CosAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	var errs []string
	for _, object := range objects {
		_, err = c.client.Object.Delete(context.Background(), objectRel(object))
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
	}
	return
}

func (c *CosAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := c.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	if exp <= 0 {
		link = c.config.Domain + objectAbs(object)
		return
	}

	var u *url.URL
	u, err = c.client.Object.GetPresignedURL(context.Background(),
		http.MethodGet, objectRel(object),
		c.config.AccessKey, c.config.SecretKey,
		time.Duration(exp)*time.Second, nil)
	if err != nil {
		return
	}
	link = u.String()
	if !strings.HasPrefix(link, c.config.Domain) {
		link = c.config.Domain + u.RequestURI()
	}
	return
}

func (c *CosAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	result, err := c.client.Object.Get(ctx, objectRel(object), nil)
	if err != nil {
		return
	}
	body = result.Body
	return
}

func (c *CosAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	var resp *cos.Response
	path := objectRel(object)
	resp, err = c.client.Object.Get(ctx, path, nil)
	if err != nil {
		return
	}
	defer func() {
		resp.Body.Close()
	}()
	header := make(map[string]string)
	for k := range resp.Header {
		header[k] = resp.Header.Get(k)
	}
	info = &File{
		Header: header,
		Name:   path,
	}
	info.ModTime, _ = time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	info.Size, _ = strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	info.IsDir = info.Size == 0
	return
}

func (c *CosAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	// TODO: 腾讯云的SDK中暂时没开放这个功能
	return
}
