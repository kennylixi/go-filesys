package filesys

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/net/gclient"
	"github.com/gogf/gf/v2/os/gctx"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"github.com/qiniu/go-sdk/v7/auth/qbox"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/qiniu/go-sdk/v7/storage"
)

type ConfigQiniu struct {
	AccessKey string `json:"accessKey" v:"required#AccessKey不能为空"`
	SecretKey string `json:"secretKey" v:"required#SecretKey不能为空"`
	Endpoint  string `json:"endpoint" v:"required#Endpoint不能为空"`
	Bucket    string `json:"bucket" v:"required#Bucket不能为空"`
	Domain    string `json:"domain"`
	Expire    int64  `json:"expire"`
}

type QiniuAdapter struct {
	config        *ConfigQiniu
	zone          *storage.Zone
	mac           *qbox.Mac
	bucketManager *storage.BucketManager
}

func NewAdapterQiniu(i interface{}) (Adapter, error) {
	cfg := (*ConfigQiniu)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}
	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/")
	q := &QiniuAdapter{
		config: cfg,
	}

	q.mac = qbox.NewMac(cfg.AccessKey, cfg.SecretKey)
	var err error
	q.zone, err = storage.GetZone(cfg.AccessKey, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	q.bucketManager = storage.NewBucketManager(q.mac, &storage.Config{Zone: q.zone})
	return q, err
}

func (q *QiniuAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = q.GetInfo(ctx, object)
	return
}

// Upload TODO: 目前没发现有可以设置header的地方
func (q *QiniuAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	policy := storage.PutPolicy{Scope: q.config.Bucket}
	token := policy.UploadToken(q.mac)
	cfg := &storage.Config{
		Zone: q.zone,
	}
	form := storage.NewFormUploader(cfg)
	ret := &storage.PutRet{}
	params := make(map[string]string)
	for _, header := range headers {
		for k, v := range header {
			params["x:"+k] = v
		}
	}
	extra := &storage.PutExtra{
		Params: params,
	}
	path = objectRel(path)
	// 需要先删除，文件已存在的话，没法覆盖
	q.Delete(ctx, path)
	err = form.Put(context.Background(), ret, token, path, reader, size, extra)
	return
}

func (q *QiniuAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	length := len(objects)
	if length == 0 {
		return
	}

	defer func() {
		// 被删除文件不存在的时候，err值为空但不为nil，这里处理一下
		if err != nil && err.Error() == "" {
			err = nil
		}
	}()

	deleteOps := make([]string, 0, length)
	for _, object := range objects {
		deleteOps = append(deleteOps, storage.URIDelete(q.config.Bucket, objectRel(object)))
	}
	cfg := &storage.Config{
		Zone: q.zone,
	}
	manager := storage.NewBucketManager(q.mac, cfg)
	var res []storage.BatchOpRet
	res, err = manager.Batch(deleteOps)
	if err != nil {
		return
	}

	var errs []string
	for _, item := range res {
		if item.Code != http.StatusOK {
			errs = append(errs, fmt.Errorf("%+v: %v", item.Data, item.Code).Error())
		}
	}

	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
	}

	return
}

func (q *QiniuAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := q.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	object = objectRel(object)
	if exp > 0 {
		deadline := time.Now().Add(time.Second * time.Duration(exp)).Unix()
		link = storage.MakePrivateURL(q.mac, q.config.Domain, object, deadline)
	} else {
		link = storage.MakePublicURL(q.config.Domain, object)
	}

	if !strings.HasPrefix(link, q.config.Domain) {
		if u, errU := url.Parse(link); errU == nil {
			link = q.config.Domain + u.RequestURI()
		}
	}

	return
}

func (q *QiniuAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	link, err := q.GetSignURL(ctx, object)
	if err != nil {
		return
	}
	req := gclient.New().SetTimeout(30 * time.Minute)
	if strings.HasPrefix(strings.ToLower(link), "https://") {
		req.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	resp, err := req.Get(gctx.New(), link)
	if err != nil {
		return
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, gerror.New("下载文件失败")
	}
	body = resp.Body
	return
}

func (q *QiniuAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	var fileInfo storage.FileInfo

	object = objectRel(object)
	fileInfo, err = q.bucketManager.Stat(q.config.Bucket, object)
	if err != nil {
		return
	}
	info = &File{
		Name:    object,
		Size:    fileInfo.Fsize,
		ModTime: storage.ParsePutTime(fileInfo.PutTime),
		IsDir:   fileInfo.Fsize == 0,
	}
	return
}

func (q *QiniuAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	var items []storage.ListItem

	prefix = objectRel(prefix)
	limit := 1000
	cfg := &storage.Config{
		Zone: q.zone,
	}

	manager := storage.NewBucketManager(q.mac, cfg)
	items, _, _, _, err = manager.ListFiles(q.config.Bucket, prefix, "", "", limit)
	if err != nil {
		return
	}

	for _, item := range items {
		files = append(files, &File{
			ModTime: storage.ParsePutTime(item.PutTime),
			Name:    objectRel(item.Key),
			Size:    item.Fsize,
			IsDir:   item.Fsize == 0,
		})
	}

	return
}
