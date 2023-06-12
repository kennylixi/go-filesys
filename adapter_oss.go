package filesys

import (
	"context"
	"errors"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type ConfigOss struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Endpoint  string `json:"endpoint" v:"required#Endpoint不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type OssAdapter struct {
	config *ConfigOss
	client *oss.Bucket
}

func NewAdapterOss(i interface{}) (Adapter, error) {
	cfg := (*ConfigOss)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}
	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}
	if cfg.Expire <= 0 {
		cfg.Expire = 1800
	}

	if cfg.Domain == "" {
		cfg.Domain = "https://" + cfg.Bucket + "." + cfg.Endpoint
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/ ")

	o := &OssAdapter{
		config: cfg,
	}
	client, err := oss.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey)
	if err != nil {
		return nil, err
	}
	o.client, err = client.Bucket(cfg.Bucket)
	return o, err
}

func (o *OssAdapter) IsExist(ctx context.Context, object string) (err error) {
	var b bool
	b, err = o.client.IsObjectExist(objectRel(object))
	if err != nil {
		return
	}
	if !b {
		return errors.New("file is not exist")
	}
	return
}

func (o *OssAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	var opts []oss.Option
	for _, header := range headers {
		for k, v := range header {
			switch strings.ToLower(k) {
			case "content-type":
				opts = append(opts, oss.ContentType(v))
			case "content-encoding":
				opts = append(opts, oss.ContentEncoding(v))
			case "content-disposition":
				opts = append(opts, oss.ContentDisposition(v))
				// TODO: more
			}
		}
	}
	err = o.client.PutObject(strings.TrimLeft(path, "./"), reader, opts...)
	return
}

func (o *OssAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	_, err = o.client.DeleteObjects(objects)
	return
}

func (o *OssAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := o.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	path := objectRel(object)
	if exp <= 0 {
		return o.config.Domain + "/" + path, nil
	}
	link, err = o.client.SignURL(path, http.MethodGet, exp)
	if err != nil {
		return
	}
	if !strings.HasPrefix(link, o.config.Domain) {
		if u, errU := url.Parse(link); errU == nil {
			link = o.config.Domain + u.RequestURI()
		}
	}

	return
}

func (o *OssAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	body, err = o.client.GetObject(objectRel(object))
	return
}

func (o *OssAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	// https://help.aliyun.com/document_detail/31859.html?spm=a2c4g.11186623.2.10.713d1592IKig7s#concept-lkf-swy-5db
	//Cache-Control	指定该 Object 被下载时的网页的缓存行为
	//Content-Disposition	指定该 Object 被下载时的名称
	//Content-Encoding	指定该 Object 被下载时的内容编码格式
	//Content-Language	指定该 Object 被下载时的内容语言编码
	//Expires	过期时间
	//Content-Length	该 Object 大小
	//Content-Type	该 Object 文件类型
	//Last-Modified	最近修改时间

	var header http.Header

	path := objectRel(object)
	header, err = o.client.GetObjectMeta(path)
	if err != nil {
		return
	}

	headerMap := make(map[string]string)

	for k, _ := range header {
		headerMap[k] = header.Get(k)
	}
	info = &File{}
	info.Header = headerMap
	info.Size, _ = strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	info.ModTime, _ = time.Parse(http.TimeFormat, header.Get("Last-Modified"))
	info.Name = path
	info.IsDir = false
	return
}

func (o *OssAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	prefix = objectRel(prefix)

	var res oss.ListObjectsResult

	res, err = o.client.ListObjects(oss.Prefix(prefix))
	if err != nil {
		return
	}
	for _, object := range res.Objects {
		files = append(files, &File{
			ModTime: object.LastModified,
			Name:    object.Key,
			Size:    object.Size,
			IsDir:   object.Size == 0,
			Header:  map[string]string{},
		})
	}
	return
}
