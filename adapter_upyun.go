package filesys

import (
	"context"
	"errors"
	"fmt"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/upyun/go-sdk/upyun"
)

type ConfigUpYun struct {
	Bucket   string `json:"bucket" v:"required#AccessKey不能为空"`
	Operator string `json:"operator" v:"required#Operator不能为空"`
	Password string `json:"password" v:"required#Password不能为空"`
	Secret   string `json:"secret" v:"required#Secret不能为空"`
	Domain   string `json:"domain"`
	Expire   int64  `json:"expire"`
}

type UpYunAdapter struct {
	config *ConfigUpYun
	client *upyun.UpYun
}

func NewAdapterUpYun(i interface{}) (Adapter, error) {
	cfg := (*ConfigUpYun)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}

	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}
	if !strings.HasPrefix(cfg.Domain, "http://") && !strings.HasPrefix(cfg.Domain, "https://") {
		cfg.Domain = "http://" + cfg.Domain
	}
	cfg.Domain = strings.TrimRight(cfg.Domain, "/")
	client := upyun.NewUpYun(&upyun.UpYunConfig{
		Bucket:   cfg.Bucket,
		Operator: cfg.Operator,
		Password: cfg.Password,
	})
	return &UpYunAdapter{
		config: cfg,
		client: client,
	}, nil
}

func (u *UpYunAdapter) IsExist(ctx context.Context, object string) (err error) {
	_, err = u.client.GetInfo(objectAbs(object))
	return
}

func (u *UpYunAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	h := make(map[string]string)
	if err != nil {
		return
	}
	for _, header := range headers {
		for k, v := range header {
			h[k] = v
		}
	}
	err = u.client.Put(&upyun.PutObjectConfig{
		Path:    objectAbs(path),
		Reader:  reader,
		Headers: h,
	})
	return
}

func (u *UpYunAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	var errs []string
	for _, object := range objects {
		err = u.client.Delete(&upyun.DeleteObjectConfig{
			Path: objectAbs(object),
		})
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
	}
	return
}

// GetSignURL https://help.upyun.com/knowledge-base/cdn-token-limite/
func (u *UpYunAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	exp := u.config.Expire
	if len(expire) > 0 {
		exp = expire[0]
	}
	path := objectAbs(object)
	if exp <= 0 {
		return u.config.Domain + path, nil
	}
	endTime := time.Now().Unix() + exp
	sign := MD5Crypt(fmt.Sprintf("%v&%v&%v", u.config.Secret, endTime, path))
	sign = strings.Join(strings.Split(sign, "")[12:20], "") + fmt.Sprint(endTime)
	return u.config.Domain + path + "?_upt=" + sign, nil
}

func (u *UpYunAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	chans := make(chan *upyun.FileInfo, 1000)
	prefix = objectRel(prefix)
	u.client.List(&upyun.GetObjectsConfig{
		Path:        prefix,
		ObjectsChan: chans,
	})
	for obj := range chans {
		files = append(files, &File{
			ModTime: obj.Time,
			Size:    obj.Size,
			IsDir:   obj.IsDir,
			Header:  obj.Meta, // 注意：这里获取不到文件的header
			Name:    filepath.Join(prefix, objectRel(obj.Name)),
		})
	}
	return
}

func (u *UpYunAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	file := new(os.File)
	_, err = u.client.Get(&upyun.GetObjectConfig{
		Path:   objectAbs(object),
		Writer: file,
	})
	body = file
	if err != nil {
		return
	}
	return
}

func (u *UpYunAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	var fileInfo *upyun.FileInfo
	fileInfo, err = u.client.GetInfo(objectAbs(object))
	if err != nil {
		return
	}
	info = &File{
		ModTime: fileInfo.Time,
		Name:    objectRel(fileInfo.Name),
		Size:    fileInfo.Size,
		IsDir:   fileInfo.IsDir,
		Header:  fileInfo.Meta,
	}
	return
}
