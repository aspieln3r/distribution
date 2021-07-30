package ipfs

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"

	// "path/filepath"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"

	shell "github.com/ipfs/go-ipfs-api"
	// unixfs "github.com/ipfs/go-unixfs"
	_ "github.com/mattn/go-sqlite3"
)

var sh *shell.Shell
var database *sql.DB
var rootdir string

func getImageName(filepath string) string {
	filepath = strings.ReplaceAll(filepath, "/docker/registry/v2/repositories", "")
	re := regexp.MustCompile("/([a-zA-Z-_]*)/.*")
	match := re.FindStringSubmatch(filepath)
	image := match[1]
	return image
}

func dbQueryFromHash(hash string) (string, string) {
	var filename, time string
	rows, err := database.Query("select filename, time from pinlist where hash=?", hash)
	if err != nil {
		fmt.Println("error while querying: ", err)
		return "", ""
	}
	defer rows.Close()
	rows.Next()
	if err := rows.Scan(&filename, &time); err != nil {
		return "", ""
	} else {
		return filename, time
	}
}

func dbQueryFromPath(path string) (string, string, error) {
	var filename, time string
	rows, err := database.Query("select hash, time from pinlist where filename=?", path)
	if err != nil {
		fmt.Println("error while querying: ", err)
		return "", "", fmt.Errorf("file not found in db")
	}
	defer rows.Close()
	rows.Next()
	if err := rows.Scan(&filename, &time); err != nil {
		return "", "", fmt.Errorf("error while scanning row")
	} else {
		return filename, time, nil
	}
}

func addToDb(cid string, path string, image string) error {
	addentry, err := database.Prepare("INSERT INTO pinlist(hash,time,filename,image) VALUES (?,datetime('now'),?,?) ON CONFLICT(filename) DO UPDATE SET hash=?, time=datetime('now'), image=image")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("addToDb(%s,%s)\n", cid, path)
		return err
	}
	if _, err = addentry.Exec(cid, path, image, cid); err != nil {
		fmt.Printf("addToDb(%s,%s)\n", cid, path)
		fmt.Println(err)
	}
	return err
}

//-------------------------------------------------------------------------------------------------

const (
	driverName           = "ipfs"
	defaultRootDirectory = "/var/lib/registry"
	defaultMaxThreads    = uint64(100)

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	RootDirectory string
	MaxThreads    uint64
}

func init() {
	factory.Register(driverName, &ipfsDriverFactory{})
}

// ipfsDriverFactory implements the factory.StorageDriverFactory interface
type ipfsDriverFactory struct{}

func (factory *ipfsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// actual storagedriver - Driver is a wrapper to manage this
type driver struct {
	rootDirectory string
}

// fullPath returns the absolute path of a key within the Driver's storage.
func (d *driver) fullPath(subPath string) string {
	return path.Join(d.rootDirectory, subPath)
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by
// IPFS. All provided paths will be subpaths of the RootDirectory.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - rootdirectory
// - maxthreads
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	params, err := fromParametersImpl(parameters)
	if err != nil || params == nil {
		return nil, err
	}
	return New(*params), nil
}

// maps parameters string to the DriverParameters struct
func fromParametersImpl(parameters map[string]interface{}) (*DriverParameters, error) {
	var (
		err           error
		maxThreads    = defaultMaxThreads
		rootDirectory = defaultRootDirectory
	)

	if parameters != nil {
		if rootDir, ok := parameters["rootdirectory"]; ok {
			rootDirectory = fmt.Sprint(rootDir)
		}

		maxThreads, err = base.GetLimitFromParameter(parameters["maxthreads"], minThreads, defaultMaxThreads)
		if err != nil {
			return nil, fmt.Errorf("maxthreads config error: %s", err.Error())
		}
	}

	params := &DriverParameters{
		RootDirectory: rootDirectory,
		MaxThreads:    maxThreads,
	}
	return params, nil
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {
	ipfsDriver := &driver{rootDirectory: params.RootDirectory}
	rootdir = params.RootDirectory
	fmt.Printf("\nIPFS driver activated\n\troot:%s\n\n", params.RootDirectory)
	sh = shell.NewShell("127.0.0.1:5001")
	dblocation := params.RootDirectory + "/pindb"
	var err error
	if database, err = sql.Open("sqlite3", dblocation); err != nil {
		panic(fmt.Errorf("could not open database: %s", err))
	}
	createdb, _ := database.Prepare("create table if not exists pinlist (hash text, time text, filename text primary key, image text)")
	createdb.Exec()
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	fmt.Println("currently pinned files:")
	pinlist, err := sh.Pins()
	if err != nil {
		panic(fmt.Errorf(err.Error()))
	}
	for hash, ptype := range pinlist {
		if ptype.Type == "recursive" {
			filename, _ := dbQueryFromHash(hash)
			fmt.Println(hash, filename)
		}
	}
	fmt.Println("------------------------------------------------------")
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(ipfsDriver, params.MaxThreads),
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	fmt.Printf("\nd.Getcontent(ctx,%s)\n", path)
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	fmt.Printf("\nd.Putcontent(ctx,%s,contents []byte)\n", subPath)
	writer, err := d.Writer(ctx, subPath, false)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, bytes.NewReader(contents))
	if err != nil {
		writer.Cancel()
		return err
	}
	return writer.Commit()
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, subPath string, offset int64) (io.ReadCloser, error) {
	fmt.Printf("\nd.Reader(ctx,%s,%d)\n", subPath, offset)
	// get only asked file
	hash, _, err := dbQueryFromPath(subPath)
	if err == nil {
		fmt.Println("db query hit")
		parentDir := path.Dir(d.fullPath(subPath))
		if err := os.MkdirAll(parentDir, 0777); err != nil {
			return nil, err
		}
		if err := sh.Get(hash, d.fullPath(subPath)); err != nil {
			fmt.Println("error on sh.Get()")
			fmt.Printf("\n%s\n", err)
			return nil, err
		}
		fmt.Printf("%s was pulled and written to %s\n", subPath, d.fullPath(subPath))
	} else {
		fmt.Println("db query miss")
	}
	/////////////////////////////////////////////////////////////////////////////////////////////////
	file, err := os.OpenFile(d.fullPath(subPath), os.O_RDONLY, 0644)
	if err != nil {
		fmt.Printf("\n%s\n", err)
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}

		return nil, err
	}
	fmt.Println("opened file: ", file.Name())
	seekPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, err
	} else if seekPos < offset {
		file.Close()
		return nil, storagedriver.InvalidOffsetError{Path: subPath, Offset: offset}
	}

	return file, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	fmt.Printf("\nd.Writer(ctx,%s,%t)\n", subPath, append)
	fullPath := d.fullPath(subPath)
	parentDir := path.Dir(fullPath)
	if err := os.MkdirAll(parentDir, 0777); err != nil {
		return nil, err
	}

	fp, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	var offset int64

	if !append {
		err := fp.Truncate(0)
		if err != nil {
			fp.Close()
			return nil, err
		}
	} else {
		n, err := fp.Seek(0, io.SeekEnd)
		if err != nil {
			fp.Close()
			return nil, err
		}
		offset = n
	}

	return newFileWriter(fp, offset), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	fmt.Printf("\nd.Stat(ctx,%s)\n", subPath)
	hash, _, err := dbQueryFromPath(subPath)
	if err == nil {
		fmt.Println("db query hit")
		fullPath := d.fullPath(subPath)
		_, err := os.Stat(fullPath)
		if err != nil {
			// if directory doesnt exist on filesystem cache, pull from ipfs
			if os.IsNotExist(err) {
				parentDir := path.Dir(d.fullPath(subPath))
				if err := os.MkdirAll(parentDir, 0777); err != nil {
					return nil, err
				}
				if err := sh.Get(hash, d.fullPath(subPath)); err != nil {
					fmt.Println("error on sh.Get()")
					fmt.Printf("\n%s\n", err)
					return nil, err
				}
				fmt.Printf("%s was pulled and written to %s\n", subPath, d.fullPath(subPath))
			}
		}
	} else {
		fmt.Println("db query miss")
	}

	fullPath := d.fullPath(subPath)

	fi, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}

		return nil, err
	}

	return fileInfo{
		path:     subPath,
		FileInfo: fi,
	}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	fmt.Printf("\nd.List(ctx,%s)\n", subPath)
	fullPath := d.fullPath(subPath)

	dir, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		return nil, err
	}

	defer dir.Close()

	fileNames, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		keys = append(keys, path.Join(subPath, fileName))
	}

	return keys, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	fmt.Printf("\nd.List(ctx,%s,%s)\n", sourcePath, destPath)
	source := d.fullPath(sourcePath)
	dest := d.fullPath(destPath)

	if _, err := os.Stat(source); os.IsNotExist(err) {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}

	if err := os.MkdirAll(path.Dir(dest), 0777); err != nil {
		return err
	}

	err := os.Rename(source, dest)
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
	fmt.Printf("\nd.Delete(ctx,%s)\n", subPath)
	fullPath := d.fullPath(subPath)

	_, err := os.Stat(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err != nil {
		return storagedriver.PathNotFoundError{Path: subPath}
	}

	err = os.RemoveAll(fullPath)
	return err
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	fmt.Printf("\nd.URLFor(ctx,%s,%s)\n", path, options)
	return "", storagedriver.ErrUnsupportedMethod{}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	fmt.Printf("\nd.Walk(ctx,%s,walkfn)\n", path)
	return storagedriver.WalkFallback(ctx, d, path, f)
}

type fileInfo struct {
	os.FileInfo
	path string
}

var _ storagedriver.FileInfo = fileInfo{}

// Path provides the full path of the target of this file info.
func (fi fileInfo) Path() string {
	return fi.path
}

// Size returns current length in bytes of the file. The return value can
// be used to write to the end of the file at path. The value is
// meaningless if IsDir returns true.
func (fi fileInfo) Size() int64 {
	if fi.IsDir() {
		return 0
	}

	return fi.FileInfo.Size()
}

// ModTime returns the modification time for the file. For backends that
// don't have a modification time, the creation time should be returned.
func (fi fileInfo) ModTime() time.Time {
	return fi.FileInfo.ModTime()
}

// IsDir returns true if the path is a directory.
func (fi fileInfo) IsDir() bool {
	return fi.FileInfo.IsDir()
}

type fileWriter struct {
	file      *os.File
	size      int64
	bw        *bufio.Writer
	closed    bool
	committed bool
	cancelled bool
}

func newFileWriter(file *os.File, size int64) *fileWriter {
	return &fileWriter{
		file: file,
		size: size,
		bw:   bufio.NewWriter(file),
	}
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	fmt.Printf("\nfw.Write()")
	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.committed {
		return 0, fmt.Errorf("already committed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}
	n, err := fw.bw.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

//pin file after close()
func (fw *fileWriter) Close() error {
	fmt.Printf("\nfw.Close(%s)\n", fw.file.Name())
	if fw.closed {
		return fmt.Errorf("already closed")
	}
	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}
	fname := fw.file.Name()
	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.closed = true
	file, _ := os.Open(fname)
	fname = strings.ReplaceAll(fname, rootdir, "")
	cid, err := sh.Add(file)
	if err != nil {
		fmt.Printf("\n%s error adding to ipfs\n", fname)
		return fmt.Errorf("error adding file to ipfs")
	}
	image := getImageName(fname)
	err = addToDb(cid, fname, image)
	if err != nil {
		fmt.Printf("\n%s error adding to database\n", fname)
		return fmt.Errorf("error adding file entry to database")
	}
	fmt.Printf("\n%s added to ipfs\n", fname)
	return nil
}

func (fw *fileWriter) Cancel() error {
	fmt.Printf("\nfw.Cancel()")
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.cancelled = true
	fw.file.Close()
	return os.Remove(fw.file.Name())
}

func (fw *fileWriter) Commit() error {
	fmt.Printf("\nfw.Commit()")
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	fw.committed = true
	return nil
}
