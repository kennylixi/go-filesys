package filesys

import (
	"context"
	"github.com/gogf/gf/v2/errors/gerror"
)

const (
	TypeBos   = "bos"   //百度云存储
	TypeCos   = "cos"   //腾讯云存储
	TypeLocal = "local" //本地
	TypeMinio = "minio" //minio存储
	TypeObs   = "obs"   //华为云存储
	TypeOss   = "oss"   //阿里云存储
	TypeQiniu = "qiniu" //七牛云储存
	TypeUpyun = "upyun" //又拍云存储
)

type NewAdapter func(i interface{}) (Adapter, error)

var (
	defaultStore *Store // 默认文件存储器
	adapters     = map[string]NewAdapter{
		TypeBos:   NewAdapterBos,
		TypeCos:   NewAdapterCos,
		TypeLocal: NewAdapterLocal,
		TypeMinio: NewAdapterMinio,
		TypeObs:   NewAdapterObs,
		TypeOss:   NewAdapterOss,
		TypeQiniu: NewAdapterQiniu,
		TypeUpyun: NewAdapterUpYun,
	}
)

// Init 根据配置信息初始化驱动，并且设置为默认驱动
func Init(ctx context.Context, adapterType string, cfg interface{}) (err error) {
	adapterFun, ok := adapters[adapterType]
	if !ok || adapterFun == nil {
		return gerror.Newf("适配器[%s]不存在", adapterType)
	}
	defaultStore, err = NewStore(adapterType, cfg)
	return err
}
