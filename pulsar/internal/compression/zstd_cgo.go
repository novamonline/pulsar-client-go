package compression

import (
	"github.com/DataDog/zstd"
	log "github.com/sirupsen/logrus"
)

type zstdCGoProvider struct {
	ctx       zstd.Ctx
	level     Level
	zstdLevel int
}

func newCGoZStdProvider(level Level) Provider {
	z := &zstdCGoProvider{
		ctx: zstd.NewCtx(),
	}

	switch level {
	case Default:
		z.zstdLevel = zstd.DefaultCompression
	case Faster:
		z.zstdLevel = zstd.BestSpeed
	case Better:
		z.zstdLevel = 9
	}

	return z
}

func NewZStdProvider(level Level) Provider {
	return newCGoZStdProvider(level)
}

func (z *zstdCGoProvider) CompressMaxSize(originalSize int) int {
	return zstd.CompressBound(originalSize)
}

func (z *zstdCGoProvider) Compress(dst, src []byte) []byte {
	out, err := z.ctx.CompressLevel(dst, src, z.zstdLevel)
	if err != nil {
		log.WithError(err).Fatal("Failed to compress")
	}

	return out
}

func (z *zstdCGoProvider) Decompress(dst, src []byte, originalSize int) ([]byte, error) {
	return z.ctx.Decompress(dst, src)
}

func (z *zstdCGoProvider) Close() error {
	return nil
}

func (z *zstdCGoProvider) Clone() Provider {
	return newCGoZStdProvider(z.level)
}
