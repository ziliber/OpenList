package op

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	stdpath "path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/archive/tool"
	"github.com/OpenListTeam/OpenList/v4/internal/cache"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/singleflight"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	gocache "github.com/OpenListTeam/go-cache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var archiveMetaCache = gocache.NewMemCache(gocache.WithShards[*model.ArchiveMetaProvider](64))
var archiveMetaG singleflight.Group[*model.ArchiveMetaProvider]

func GetArchiveMeta(ctx context.Context, storage driver.Driver, path string, args model.ArchiveMetaArgs) (*model.ArchiveMetaProvider, error) {
	if storage.Config().CheckStatus && storage.GetStorage().Status != WORK {
		return nil, errors.WithMessagef(errs.StorageNotInit, "storage status: %s", storage.GetStorage().Status)
	}
	path = utils.FixAndCleanPath(path)
	key := Key(storage, path)
	fn := func() (*model.ArchiveMetaProvider, error) {
		_, m, err := getArchiveMeta(ctx, storage, path, args)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get %s archive met: %+v", path, err)
		}
		if m.Expiration != nil {
			archiveMetaCache.Set(key, m, gocache.WithEx[*model.ArchiveMetaProvider](*m.Expiration))
		}
		return m, nil
	}
	// if storage.Config().NoLinkSingleflight {
	// 	meta, err := fn()
	// 	return meta, err
	// }
	if !args.Refresh {
		if meta, ok := archiveMetaCache.Get(key); ok {
			log.Debugf("use cache when get %s archive meta", path)
			return meta, nil
		}
	}
	meta, err, _ := archiveMetaG.Do(key, fn)
	return meta, err
}

func GetArchiveToolAndStream(ctx context.Context, storage driver.Driver, path string, args model.LinkArgs) (model.Obj, tool.Tool, []*stream.SeekableStream, error) {
	l, obj, err := Link(ctx, storage, path, args)
	if err != nil {
		return nil, nil, nil, errors.WithMessagef(err, "failed get [%s] link", path)
	}
	baseName, ext, found := strings.Cut(obj.GetName(), ".")
	if !found {
		_ = l.Close()
		return nil, nil, nil, errors.Errorf("failed get archive tool: the obj does not have an extension.")
	}
	partExt, t, err := tool.GetArchiveTool("." + ext)
	if err != nil {
		var e error
		partExt, t, e = tool.GetArchiveTool(stdpath.Ext(obj.GetName()))
		if e != nil {
			_ = l.Close()
			return nil, nil, nil, errors.WithMessagef(stderrors.Join(err, e), "failed get archive tool: %s", ext)
		}
	}
	ss, err := stream.NewSeekableStream(&stream.FileStream{Ctx: ctx, Obj: obj}, l)
	if err != nil {
		_ = l.Close()
		return nil, nil, nil, errors.WithMessagef(err, "failed get [%s] stream", path)
	}
	ret := []*stream.SeekableStream{ss}
	if partExt == nil {
		return obj, t, ret, nil
	} else {
		index := partExt.SecondPartIndex
		dir := stdpath.Dir(path)
		for {
			p := stdpath.Join(dir, baseName+fmt.Sprintf(partExt.PartFileFormat, index))
			var o model.Obj
			l, o, err = Link(ctx, storage, p, args)
			if err != nil {
				break
			}
			ss, err = stream.NewSeekableStream(&stream.FileStream{Ctx: ctx, Obj: o}, l)
			if err != nil {
				_ = l.Close()
				for _, s := range ret {
					_ = s.Close()
				}
				return nil, nil, nil, errors.WithMessagef(err, "failed get [%s] stream", path)
			}
			ret = append(ret, ss)
			index++
		}
		return obj, t, ret, nil
	}
}

func getArchiveMeta(ctx context.Context, storage driver.Driver, path string, args model.ArchiveMetaArgs) (model.Obj, *model.ArchiveMetaProvider, error) {
	storageAr, ok := storage.(driver.ArchiveReader)
	if ok {
		obj, err := GetUnwrap(ctx, storage, path)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "failed to get file")
		}
		if obj.IsDir() {
			return nil, nil, errors.WithStack(errs.NotFile)
		}
		meta, err := storageAr.GetArchiveMeta(ctx, obj, args.ArchiveArgs)
		if !errors.Is(err, errs.NotImplement) {
			archiveMetaProvider := &model.ArchiveMetaProvider{ArchiveMeta: meta, DriverProviding: true}
			if meta != nil && meta.GetTree() != nil {
				archiveMetaProvider.Sort = &storage.GetStorage().Sort
			}
			if !storage.Config().NoCache {
				Expiration := time.Minute * time.Duration(storage.GetStorage().CacheExpiration)
				archiveMetaProvider.Expiration = &Expiration
			}
			return obj, archiveMetaProvider, err
		}
	}
	obj, t, ss, err := GetArchiveToolAndStream(ctx, storage, path, args.LinkArgs)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		var e error
		for _, s := range ss {
			e = stderrors.Join(e, s.Close())
		}
		if e != nil {
			log.Errorf("failed to close file streamer, %v", e)
		}
	}()
	meta, err := t.GetMeta(ss, args.ArchiveArgs)
	if err != nil {
		return nil, nil, err
	}
	archiveMetaProvider := &model.ArchiveMetaProvider{ArchiveMeta: meta, DriverProviding: false}
	if meta.GetTree() != nil {
		archiveMetaProvider.Sort = &storage.GetStorage().Sort
	}
	if !storage.Config().NoCache {
		Expiration := time.Minute * time.Duration(storage.GetStorage().CacheExpiration)
		archiveMetaProvider.Expiration = &Expiration
	}
	return obj, archiveMetaProvider, err
}

var archiveListCache = gocache.NewMemCache(gocache.WithShards[[]model.Obj](64))
var archiveListG singleflight.Group[[]model.Obj]

func ListArchive(ctx context.Context, storage driver.Driver, path string, args model.ArchiveListArgs) ([]model.Obj, error) {
	if storage.Config().CheckStatus && storage.GetStorage().Status != WORK {
		return nil, errors.WithMessagef(errs.StorageNotInit, "storage status: %s", storage.GetStorage().Status)
	}
	path = utils.FixAndCleanPath(path)
	metaKey := Key(storage, path)
	key := stdpath.Join(metaKey, args.InnerPath)
	if !args.Refresh {
		if files, ok := archiveListCache.Get(key); ok {
			log.Debugf("use cache when list archive [%s]%s", path, args.InnerPath)
			return files, nil
		}
		// if meta, ok := archiveMetaCache.Get(metaKey); ok {
		// 	log.Debugf("use meta cache when list archive [%s]%s", path, args.InnerPath)
		// 	return getChildrenFromArchiveMeta(meta, args.InnerPath)
		// }
	}
	objs, err, _ := archiveListG.Do(key, func() ([]model.Obj, error) {
		obj, files, err := listArchive(ctx, storage, path, args)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list archive [%s]%s: %+v", path, args.InnerPath, err)
		}
		// set path
		for _, f := range files {
			if s, ok := f.(model.SetPath); ok && f.GetPath() == "" && obj.GetPath() != "" {
				s.SetPath(stdpath.Join(obj.GetPath(), args.InnerPath, f.GetName()))
			}
		}
		// warp obj name
		model.WrapObjsName(files)
		// sort objs
		if storage.Config().LocalSort {
			model.SortFiles(files, storage.GetStorage().OrderBy, storage.GetStorage().OrderDirection)
		}
		model.ExtractFolder(files, storage.GetStorage().ExtractFolder)
		if !storage.Config().NoCache {
			if len(files) > 0 {
				log.Debugf("set cache: %s => %+v", key, files)
				archiveListCache.Set(key, files, gocache.WithEx[[]model.Obj](time.Minute*time.Duration(storage.GetStorage().CacheExpiration)))
			} else {
				log.Debugf("del cache: %s", key)
				archiveListCache.Del(key)
			}
		}
		return files, nil
	})
	return objs, err
}

func _listArchive(ctx context.Context, storage driver.Driver, path string, args model.ArchiveListArgs) (model.Obj, []model.Obj, error) {
	storageAr, ok := storage.(driver.ArchiveReader)
	if ok {
		obj, err := GetUnwrap(ctx, storage, path)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "failed to get file")
		}
		if obj.IsDir() {
			return nil, nil, errors.WithStack(errs.NotFile)
		}
		files, err := storageAr.ListArchive(ctx, obj, args.ArchiveInnerArgs)
		if !errors.Is(err, errs.NotImplement) {
			return obj, files, err
		}
	}
	obj, t, ss, err := GetArchiveToolAndStream(ctx, storage, path, args.LinkArgs)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		var e error
		for _, s := range ss {
			e = stderrors.Join(e, s.Close())
		}
		if e != nil {
			log.Errorf("failed to close file streamer, %v", e)
		}
	}()
	files, err := t.List(ss, args.ArchiveInnerArgs)
	return obj, files, err
}

func listArchive(ctx context.Context, storage driver.Driver, path string, args model.ArchiveListArgs) (model.Obj, []model.Obj, error) {
	obj, files, err := _listArchive(ctx, storage, path, args)
	if errors.Is(err, errs.NotSupport) {
		var meta model.ArchiveMeta
		meta, err = GetArchiveMeta(ctx, storage, path, model.ArchiveMetaArgs{
			ArchiveArgs: args.ArchiveArgs,
			Refresh:     args.Refresh,
		})
		if err != nil {
			return nil, nil, err
		}
		files, err = getChildrenFromArchiveMeta(meta, args.InnerPath)
		if err != nil {
			return nil, nil, err
		}
	}
	if err == nil && obj == nil {
		obj, err = GetUnwrap(ctx, storage, path)
	}
	if err != nil {
		return nil, nil, err
	}
	return obj, files, err
}

func getChildrenFromArchiveMeta(meta model.ArchiveMeta, innerPath string) ([]model.Obj, error) {
	obj := meta.GetTree()
	if obj == nil {
		return nil, errors.WithStack(errs.NotImplement)
	}
	dirs := splitPath(innerPath)
	for _, dir := range dirs {
		var next model.ObjTree
		for _, c := range obj {
			if c.GetName() == dir {
				next = c
				break
			}
		}
		if next == nil {
			return nil, errors.WithStack(errs.ObjectNotFound)
		}
		if !next.IsDir() || next.GetChildren() == nil {
			return nil, errors.WithStack(errs.NotFolder)
		}
		obj = next.GetChildren()
	}
	return utils.SliceConvert(obj, func(src model.ObjTree) (model.Obj, error) {
		return src, nil
	})
}

func splitPath(path string) []string {
	var parts []string
	for {
		dir, file := stdpath.Split(path)
		if file == "" {
			break
		}
		parts = append([]string{file}, parts...)
		path = strings.TrimSuffix(dir, "/")
	}
	return parts
}

func ArchiveGet(ctx context.Context, storage driver.Driver, path string, args model.ArchiveListArgs) (model.Obj, model.Obj, error) {
	if storage.Config().CheckStatus && storage.GetStorage().Status != WORK {
		return nil, nil, errors.WithMessagef(errs.StorageNotInit, "storage status: %s", storage.GetStorage().Status)
	}
	path = utils.FixAndCleanPath(path)
	af, err := GetUnwrap(ctx, storage, path)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get file")
	}
	if af.IsDir() {
		return nil, nil, errors.WithStack(errs.NotFile)
	}
	if g, ok := storage.(driver.ArchiveGetter); ok {
		obj, err := g.ArchiveGet(ctx, af, args.ArchiveInnerArgs)
		if err == nil {
			return af, model.WrapObjName(obj), nil
		}
	}

	if utils.PathEqual(args.InnerPath, "/") {
		return af, &model.ObjWrapName{
			Name: RootName,
			Obj: &model.Object{
				Name:     af.GetName(),
				Path:     af.GetPath(),
				ID:       af.GetID(),
				Size:     af.GetSize(),
				Modified: af.ModTime(),
				IsFolder: true,
			},
		}, nil
	}

	innerDir, name := stdpath.Split(args.InnerPath)
	args.InnerPath = strings.TrimSuffix(innerDir, "/")
	files, err := ListArchive(ctx, storage, path, args)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed get parent list")
	}
	for _, f := range files {
		if f.GetName() == name {
			return af, f, nil
		}
	}
	return nil, nil, errors.WithStack(errs.ObjectNotFound)
}

type objWithLink struct {
	link *model.Link
	obj  model.Obj
}

var extractCache = cache.NewKeyedCache[*objWithLink](5 * time.Minute)
var extractG = singleflight.Group[*objWithLink]{}

func DriverExtract(ctx context.Context, storage driver.Driver, path string, args model.ArchiveInnerArgs) (*model.Link, model.Obj, error) {
	if storage.Config().CheckStatus && storage.GetStorage().Status != WORK {
		return nil, nil, errors.WithMessagef(errs.StorageNotInit, "storage status: %s", storage.GetStorage().Status)
	}
	key := stdpath.Join(Key(storage, path), args.InnerPath)
	if ol, ok := extractCache.Get(key); ok {
		if ol.link.Expiration != nil || ol.link.SyncClosers.AcquireReference() || !ol.link.RequireReference {
			return ol.link, ol.obj, nil
		}
	}

	fn := func() (*objWithLink, error) {
		ol, err := driverExtract(ctx, storage, path, args)
		if err != nil {
			return nil, errors.Wrapf(err, "failed extract archive")
		}
		if ol.link.Expiration != nil {
			extractCache.SetWithTTL(key, ol, *ol.link.Expiration)
		} else {
			extractCache.SetWithExpirable(key, ol, &ol.link.SyncClosers)
		}
		return ol, nil
	}

	for {
		ol, err, _ := extractG.Do(key, fn)
		if err != nil {
			return nil, nil, err
		}
		if ol.link.SyncClosers.AcquireReference() || !ol.link.RequireReference {
			return ol.link, ol.obj, nil
		}
	}
}

func driverExtract(ctx context.Context, storage driver.Driver, path string, args model.ArchiveInnerArgs) (*objWithLink, error) {
	storageAr, ok := storage.(driver.ArchiveReader)
	if !ok {
		return nil, errs.DriverExtractNotSupported
	}
	archiveFile, extracted, err := ArchiveGet(ctx, storage, path, model.ArchiveListArgs{
		ArchiveInnerArgs: args,
		Refresh:          false,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get file")
	}
	if extracted.IsDir() {
		return nil, errors.WithStack(errs.NotFile)
	}
	link, err := storageAr.Extract(ctx, archiveFile, args)
	return &objWithLink{link: link, obj: extracted}, err
}

type streamWithParent struct {
	rc      io.ReadCloser
	parents []*stream.SeekableStream
}

func (s *streamWithParent) Read(p []byte) (int, error) {
	return s.rc.Read(p)
}

func (s *streamWithParent) Close() error {
	err := s.rc.Close()
	for _, ss := range s.parents {
		err = stderrors.Join(err, ss.Close())
	}
	return err
}

func InternalExtract(ctx context.Context, storage driver.Driver, path string, args model.ArchiveInnerArgs) (io.ReadCloser, int64, error) {
	_, t, ss, err := GetArchiveToolAndStream(ctx, storage, path, args.LinkArgs)
	if err != nil {
		return nil, 0, err
	}
	rc, size, err := t.Extract(ss, args)
	if err != nil {
		var e error
		for _, s := range ss {
			e = stderrors.Join(e, s.Close())
		}
		if e != nil {
			log.Errorf("failed to close file streamer, %v", e)
			err = stderrors.Join(err, e)
		}
		return nil, 0, err
	}
	return &streamWithParent{rc: rc, parents: ss}, size, nil
}

func ArchiveDecompress(ctx context.Context, storage driver.Driver, srcPath, dstDirPath string, args model.ArchiveDecompressArgs, lazyCache ...bool) error {
	if storage.Config().CheckStatus && storage.GetStorage().Status != WORK {
		return errors.WithMessagef(errs.StorageNotInit, "storage status: %s", storage.GetStorage().Status)
	}
	srcPath = utils.FixAndCleanPath(srcPath)
	dstDirPath = utils.FixAndCleanPath(dstDirPath)
	srcObj, err := GetUnwrap(ctx, storage, srcPath)
	if err != nil {
		return errors.WithMessage(err, "failed to get src object")
	}
	dstDir, err := GetUnwrap(ctx, storage, dstDirPath)
	if err != nil {
		return errors.WithMessage(err, "failed to get dst dir")
	}

	switch s := storage.(type) {
	case driver.ArchiveDecompressResult:
		var newObjs []model.Obj
		newObjs, err = s.ArchiveDecompress(ctx, srcObj, dstDir, args)
		if err == nil {
			if len(newObjs) > 0 {
				for _, newObj := range newObjs {
					Cache.addDirectoryObject(storage, dstDirPath, model.WrapObjName(newObj))
				}
			} else if !utils.IsBool(lazyCache...) {
				Cache.DeleteDirectory(storage, dstDirPath)
			}
		}
	case driver.ArchiveDecompress:
		err = s.ArchiveDecompress(ctx, srcObj, dstDir, args)
		if err == nil && !utils.IsBool(lazyCache...) {
			Cache.DeleteDirectory(storage, dstDirPath)
		}
	default:
		return errs.NotImplement
	}
	return errors.WithStack(err)
}
