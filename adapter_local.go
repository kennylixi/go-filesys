package filesys

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/gogf/gf/v2/os/gfile"
	"github.com/gogf/gf/v2/util/gconv"
	"github.com/gogf/gf/v2/util/gvalid"
	"io"
	"os"
	"strings"
)

type ConfigLocal struct {
	Path   string `json:"path" v:"required#Path不能为空"`
	IsDev  string `json:"isDev"  v:"required#IsDev不能为空"`
	Domain string `json:"domain"  v:"required#Domain不能为空"`
}

type LocalAdapter struct {
	config *ConfigLocal
}

func NewAdapterLocal(i interface{}) (Adapter, error) {
	cfg := (*ConfigLocal)(nil)
	if err := gconv.Scan(i, &cfg); err != nil {
		return nil, err
	}

	if verr := gvalid.New().Data(&cfg).Run(context.Background()); verr != nil {
		if err := verr.FirstError(); err != nil {
			return nil, err
		}
	}

	c := &LocalAdapter{
		config: cfg,
	}
	if err := gfile.Mkdir(cfg.Path); err != nil {
		return nil, err
	}
	if err := gfile.Chmod(cfg.Path, os.FileMode(0776)); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *LocalAdapter) IsExist(ctx context.Context, object string) (err error) {
	exist := gfile.Exists(fmt.Sprintf("%s/%s", c.config.Path, object))
	if !exist {
		return errors.New("not exist file")
	}
	return
}

func (c *LocalAdapter) Upload(ctx context.Context, path string, reader io.Reader, size int64, headers ...map[string]string) (err error) {
	savePath := gfile.Join(c.config.Path, path)

	file, err := gfile.Create(savePath)
	if err != nil {
		return err
	}
	gfile.Chmod(savePath, os.FileMode(0666))
	bufWriter := bufio.NewWriter(file)
	bufWriter.ReadFrom(reader)
	bufWriter.Flush()
	file.Close()
	return
}

func (c *LocalAdapter) Delete(ctx context.Context, objects ...string) (err error) {
	var errs []string
	for _, object := range objects {
		filePath := gfile.Join(c.config.Path, object)
		err = gfile.Remove(filePath)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
	}
	return
}

func (c *LocalAdapter) GetSignURL(ctx context.Context, object string, expire ...int64) (link string, err error) {
	link = gfile.Join(c.config.Domain, object)
	return
}

func (c *LocalAdapter) Download(ctx context.Context, object string) (body io.ReadCloser, err error) {
	filePath := gfile.Join(c.config.Path, object)
	body, err = gfile.Open(filePath)
	return
}

func (c *LocalAdapter) GetInfo(ctx context.Context, object string) (info *File, err error) {
	filePath := gfile.Join(c.config.Path, object)
	file, err := gfile.Open(filePath)
	if err != nil {
		return
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	info = &File{
		Name: object,
	}
	info.ModTime = fileInfo.ModTime()
	info.Size = fileInfo.Size()
	info.IsDir = fileInfo.IsDir()
	return
}

func (c *LocalAdapter) Lists(ctx context.Context, prefix string) (files []*File, err error) {
	return
}
