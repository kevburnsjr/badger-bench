package main

import (
	"context"
	"flag"
	"fmt"
	"bytes"
	"crypto/sha1"
	"log"
	"math"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/trace"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/boltdb/bolt"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger-bench/store"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/y"
	"github.com/paulbellamy/ratecounter"
	"github.com/pkg/profile"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const mil float64 = 1000000

const MDB_INTEGERDUP = 0x20

var (
	which     = flag.String("kv", "", "Which KV store to use. Options: badger, rocksdb, bolt, leveldb, lmdb")
	numKeys   = flag.Float64("keys_mil", 10.0, "How many million keys to write.")
	valueSize = flag.Int("valsz", 128, "Value size in bytes.")
	keySize   = flag.Int("keysz", 16, "Key size in bytes.")
	dir       = flag.String("dir", "", "Base dir for writes.")
	history   = flag.Bool("history", false, "Store history.")
	ldbhist   = flag.String("ldbhist", "", "LevelDB History Directory.")
	mode      = flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")
	withRead  = flag.Bool("read", false, "Read each key prior to write.")
	keypfx    = flag.Int("keypfx", 3, "Bucket key size")
	keytrunc  = flag.Int("keytrunc", 0, "Truncate n bits from bucket key")
	readOther = flag.Bool("read-other", false, "Read an unrelated key prior to each write.")
	seqKeys   = flag.Bool("keys_seq", false, "Use sequential keys.")
	batchSize = flag.Int("batchsize", 1000, "Batch Size")
	tsonly    = flag.Bool("tsonly", false, "Store timeseries only")
	syncWrite = flag.Bool("sync", false, "Strong durability")
	syncMeta  = flag.Bool("syncmeta", false, "Strong durability for metadata")
	syncEvery = flag.Int("sync-every", 0, "Number of batches after which to force sync")
	n         = flag.Int("n", 1, "Number of concurrent writers")
	a         = flag.Bool("a", false, "Do not delete existing database (append)")
	i         = flag.Int("i", 10, "Stats interval")
	partition = flag.Bool("p", false, "Partition per writer")
	subparts  = flag.Int("sp", 1, "Subpartitions per writer")
	fill      = flag.Bool("fill", false, "Fill entire keyspace")
	intdup    = flag.Bool("intdup", false, "MDB_INTEGERDUP")
	delmax    = flag.Int("delmax", 100000, "Maximum number of records to remove in a delete transaction")
	exp       = flag.Duration("exp", 0, "Seconds after which an item expires")
)

type entry struct {
	Key   []byte
	Value []byte
	Meta  byte
}

func Reverse(s string) string {
    runes := []rune(s)
    for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
        runes[i], runes[j] = runes[j], runes[i]
    }
    return string(runes)
}

func fillEntry(e, prev *entry, workerNum, batchNum, entryNum int) {
	// k := rand.Int() % int(*numKeys*mil)
	// key := fmt.Sprintf("vsz=%05d-k=%010d", *valueSize, k) // 22 bytes.
	// if cap(e.Key) < len(key) {
		// e.Key = make([]byte, 2*len(key))
	// }
	// e.Key = e.Key[:len(key)]
	// copy(e.Key, key)
	if *seqKeys {
		binary.BigEndian.PutUint64(e.Key, uint64(time.Now().UnixNano()))
	} else {
		// rand.Read(e.Key)
		if prev == nil {
			rand.Read(e.Key)
		} else {
			a := sha1.Sum(prev.Key)
			e.Key = a[:keylen]
		}
	}
	// Fill is used to distribute keys sequentially over entire key space. You should see 0
	// scans while using fill until you wrap around the keyspace at which point the number of
	// scans should be exactly equal to the number of insertions.
	if *fill {
		if *keypfx == 3 {
			binary.BigEndian.PutUint16(e.Key[1:*keypfx], uint16(batchNum * *batchSize + entryNum) << *keytrunc)
		} else if *keypfx == 4 {
			binary.BigEndian.PutUint32(e.Key[:*keypfx], uint32(batchNum * *batchSize + entryNum) << *keytrunc)
		}
	} else {
		e.Key[*keypfx-1] = e.Key[*keypfx-1] << *keytrunc
	}
	if *n > 0 {
		e.Key[0] = byte(workerNum)
	}
	rand.Read(e.Value)
	e.Meta = 0
}

var bdb *badger.DB

var rdb *store.Store
var rdbs = map[int]*store.Store{}
var rdbhistory *store.Store
var rdbhistories = map[int]*store.Store{}
var boltdb *bolt.DB
var ldb *leveldb.DB
var ldbhistories = map[int]*leveldb.DB{}
var lmdbEnv *lmdb.Env
var lmdbEnvHistory *lmdb.Env
var lmdbDBI lmdb.DBI
var lmdbDBIHistory lmdb.DBI
var lmdbEnvs = map[int]*lmdb.Env{}
var lmdbEnvHistories = map[int]*lmdb.Env{}
var lmdbDBIs = map[int]lmdb.DBI{}
var lmdbDBIHistories = map[int]lmdb.DBI{}
var interval = time.Duration(*i)
var scan = ratecounter.NewRateCounter(interval * time.Second)
var deletions = ratecounter.NewRateCounter(interval * time.Second)
var keylen int
var subkeylen int
var initSize int64
var N int

func writeBatch(entries []*entry, batchNum, workerNum int) int {
	var prev *entry
	for i, e := range entries {
		fillEntry(e, prev, workerNum, batchNum, i)
		prev = e
	}
	var err error
	var keys = make([]byte, len(entries) * keylen)

	var t = time.Now()
	var ts = make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(t.UnixNano()))
	var expts = make([]byte, 8)
	binary.BigEndian.PutUint64(expts, uint64(t.Add(-1 * *exp).UnixNano()))

	var lmdbEnv = lmdbEnv
	if len(lmdbEnvs) > 0 {
		lmdbEnv = lmdbEnvs[workerNum]
	}

	subkeylen := keylen - *keypfx

	var rdb = rdb
	if len(rdbs) > 0 {
		rdb = rdbs[workerNum]
	}

	// Badger
	if bdb != nil {
		txn := bdb.NewTransaction(true)

		for i, e := range entries {
			if *withRead {
				k := e.Key
				if *readOther {
					k = []byte(Reverse(string(k)))
				}
				_, err = txn.Get(k)
				if err == nil {
					println("duplicate")
					continue
				}
			}
			if !*tsonly {
				y.Check(txn.Set(e.Key, e.Value))
			}
			copy(keys[i*keylen:i*keylen+keylen], e.Key)
		}
		if *history || *tsonly {
			txn.Set(ts, keys)
		}
		y.Check(txn.Commit())
	}

	// LevelDB
	if ldb != nil {
		batch := new(leveldb.Batch)
		for i, e := range entries {
			if *withRead {
				k := e.Key
				if *readOther {
					k = []byte(Reverse(string(k)))
				}
				_, err = ldb.Get(k, nil)
				if err == nil {
					println("duplicate")
					continue
				}
			}
			if !*tsonly {
				batch.Put(e.Key, e.Value)
			}
			copy(keys[i*keylen:i*keylen+keylen], e.Key)
		}
		wopt := &opt.WriteOptions{}
		wopt.Sync = *syncWrite
		if *history || *tsonly {
			batch.Put(ts, keys)
		}
		y.Check(ldb.Write(batch, wopt))
	}

	// RocksDB
	if rdb != nil {
		var rdbhistory = rdbhistory
		if len(rdbhistories) > 0 {
			rdbhistory = rdbhistories[workerNum]
		}
		var scanned int64
		rb := rdb.NewWriteBatch()
		defer rb.Destroy()
		for i, e := range entries {
			slice, err := rdb.Get(e.Key[0:*keypfx])
			y.Check(err)
			var v = slice.Data()
			var duplicate = false
			for j := 0; j < len(v); j += subkeylen {
				scanned++
				if bytes.Equal(v[j:j+(subkeylen)], e.Key[*keypfx:]) {
					duplicate = true
					break
				}
			}
			if duplicate {
				println("duplicate")
				continue
			}
			v = append(v, e.Key[*keypfx:]...)
			if !*tsonly {
				rb.Put(e.Key[0:*keypfx], v)
			}
			copy(keys[i*keylen:i*keylen+keylen], e.Key)
		}
		if *history || *tsonly {
			y.Check(rdbhistory.SetOne(ts, keys))
		}
		rb.Put([]byte("checkpoint"), ts)
		y.Check(rdb.WriteBatch(rb))
		scan.Incr(scanned)
	}

	// BoltDB
	if boltdb != nil {
		err := boltdb.Batch(func(txn *bolt.Tx) error {
			boltBkt := txn.Bucket([]byte("bench"))
			y.AssertTrue(boltBkt != nil)
			for i, e := range entries {
				k := e.Key
				if *readOther {
					k = []byte(Reverse(string(k)))
				}
				v := boltBkt.Get(k)
				if len(v) > 0 {
					println("duplicate")
					continue
				}
				if !*tsonly {
					if err := boltBkt.Put(e.Key, e.Value); err != nil {
						return err
					}
				}
				copy(keys[i*keylen:i*keylen+keylen], e.Key)
			}
			if *history || *tsonly {
				if err := boltBkt.Put(ts, keys); err != nil {
					return err
				}
			}
			return nil
		})
		y.Check(err)
	}

	// LMDB
	if lmdbEnv != nil {
		var lmdbDBI = lmdbDBIs[workerNum]
		var lmdbDBIHistory = lmdbDBIHistories[workerNum]
		var scanned int64
		var deleted int
		for i, e := range entries {
			copy(keys[i*keylen:i*keylen+keylen], e.Key)
		}
		err = lmdbEnv.Update(func(txn *lmdb.Txn) error {
			cur, err := txn.OpenCursor(lmdbDBI)
			if err != nil {
				return err
			}
			defer cur.Close()
			for _, e := range entries {
				if !*tsonly {
					err = cur.Put(e.Key[:*keypfx], e.Key[*keypfx:], lmdb.NoDupData)
					if err != nil {
						return err
					}
					n, _ := cur.Count()
					scanned += int64(n - 1)
				}
			}
			hcur, err := txn.OpenCursor(lmdbDBIHistory)
			if err != nil {
				return err
			}
			defer hcur.Close()
			if *exp < 1 || !*history {
				return nil
			}
			// Sweep history
			for {
				_, v, err := hcur.Get(nil, nil, lmdb.NextNoDup)
				if lmdb.IsNotFound(err) {
					break
				}
				if err != nil {
					return err
				}
				for {
					ts, vals, err := hcur.Get(nil, nil, lmdb.NextMultiple)
					if lmdb.IsNotFound(err) {
						break
					}
					if err != nil {
						return err
					}
					if bytes.Compare(ts, expts) > 0 {
						break
					}
					multi := lmdb.WrapMulti(vals, keylen)
					for i := 0; i < multi.Len(); i++ {
						v = multi.Val(i)
						err = txn.Del(lmdbDBI, v[:*keypfx], v[*keypfx:])
						if err != nil {
							if lmdb.IsNotFound(err) {
								// fmt.Printf("%x %x Not Found\n", v[:*keypfx], v[*keypfx:])
								y.Check(err)
								return err
							} else {
								y.Check(err)
								return err
							}
						}
						deleted++
					}
				}
				hcur.Del(lmdb.NoDupData)
				if float64(deleted) >= float64(len(entries)) * 1.25 {
					break
				}
			}
			// Insert history
			if err = hcur.PutMulti(ts, keys, keylen, 0); err != nil {
				return err
			}
			return nil
		})
		y.Check(err)
		if *syncEvery > 0 && batchNum % *syncEvery == 0 {
			lmdbEnv.Sync(true)
		}
		y.Check(err)
		scan.Incr(scanned)
		deletions.Incr(int64(deleted))
	}

	return len(entries)
}

func humanize(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%6.2fM", float64(n)/1000000.0)
	}
	if n >= 1000 {
		return fmt.Sprintf("%6.2fK", float64(n)/1000.0)
	}
	return fmt.Sprintf("%5.2f", float64(n))
}

func humanizeFloat(n float64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%6.2fM", n/1000000.0)
	}
	if n >= 1000 {
		return fmt.Sprintf("%6.2fK", n/1000.0)
	}
	return fmt.Sprintf("%5.2f", n)
}

type logger struct {}
func (l logger) Errorf(v string, args ...interface{}) {
	log.Printf(v, args...)
}
func (l logger) Warningf(v string, args ...interface{}) {
	log.Printf(v, args...)
}
func (l logger) Infof(v string, args ...interface{}) {}
func (l logger) Debugf(v string, args ...interface{}) {}

func main() {
	flag.Parse()
	switch *mode {
	case "cpu":
		defer profile.Start(profile.CPUProfile).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile).Stop()
	case "mutex":
		defer profile.Start(profile.MutexProfile).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile).Stop()
	default:
		// do nothing
	}

	trace.AuthRequest = func(req *http.Request) (any, sensitive bool) {
		return true, true
	}

	keylen = *keySize
	subkeylen = keylen - *keypfx

	nw := *numKeys * mil
	fmt.Printf("TOTAL KEYS TO WRITE: %s\n", humanize(int64(nw)))
	fmt.Printf("WITH VALUE SIZE: %s\n", humanize(int64(*valueSize)))
	if *withRead {
		fmt.Printf("WITH READ\n")
	}
	if *readOther {
		fmt.Printf("WITH READ OTHER\n")
	}
	if *history {
		fmt.Printf("WITH HISTORY\n")
	}
	if *tsonly {
		fmt.Printf("TIMESERIES ONLY\n")
	}
	if *seqKeys {
		fmt.Printf("WITH SEQUENTIAL KEYS\n")
	} else {
		fmt.Printf("WITH RANDOM KEYS\n")
	}
	fmt.Printf("BATCH SIZE %d\n", *batchSize)
	fmt.Printf("CONCURRENCY %d\n", *n)
	fmt.Printf("KEY SIZE %d\n", *keySize)
	fmt.Printf("KEY PREFIX %d\n", *keypfx)
	fmt.Printf("KEY SPACE %s\n", humanize(int64(*n * *subparts * int(math.Pow(2, float64((8*(*keypfx-1)-*keytrunc)))))))
	if *keytrunc > 0 {
		fmt.Printf("KEY TRUNCATE %d\n", *keytrunc)
	}
	if *partition {
		fmt.Printf("PARTITIONED\n")
	}
	if *a {
		fmt.Printf("APPEND\n")
	}
	badgerOpt := badger.DefaultOptions(*dir + "/badger")
	badgerOpt.TableLoadingMode = options.FileIO
	badgerOpt.SyncWrites = *syncWrite
	badgerOpt.Logger = logger{}

	var err error
	var init bool

	if *which == "badger" {
		init = true
		fmt.Println("Init Badger")
		if !*a {
			y.Check(os.RemoveAll(*dir + "/badger"))
			os.MkdirAll(*dir+"/badger", 0777)
		}
		bdb, err = badger.Open(badgerOpt)
		if err != nil {
			log.Fatalf("while opening badger: %v", err)
		}
	} else if *which == "rocksdb" {
		init = true
		fmt.Println("Init Rocks")
		start := time.Now()
		if *partition {
			for i := 0; i < *n; i++ {
				d := fmt.Sprintf(*dir+"/%x/rocks", i)
				if !*a {
					os.RemoveAll(d)
				}
				os.MkdirAll(d, 0777)
				if *syncWrite {
					rdbs[i], err = store.NewSyncStore(d)
				} else {
					rdbs[i], err = store.NewStore(d)
				}
				y.Check(err)
				d = fmt.Sprintf(*dir+"/%x/rocks.ts", i)
				os.MkdirAll(d, 0777)
				rdbhistories[i], err = store.NewSyncStore(d)
				y.Check(err)
			}
		} else {
			if !*a {
				os.RemoveAll(*dir + "/rocks")
				os.MkdirAll(*dir+"/rocks", 0777)
			}
			if *syncWrite {
				rdb, err = store.NewSyncStore(*dir + "/rocks")
			} else {
				rdb, err = store.NewStore(*dir + "/rocks")
			}
			y.Check(err)
			rdbhistory, err = store.NewSyncStore(*dir + "/rocks.ts")
			y.Check(err)
		}
		fmt.Printf("Initialized after %v\n", time.Now().Sub(start))
	} else if *which == "bolt" {
		init = true
		fmt.Println("Init BoltDB")
		if !*a {
			os.RemoveAll(*dir + "/bolt")
			os.MkdirAll(*dir+"/bolt", 0777)
		}
		boltdb, err = bolt.Open(*dir+"/bolt/bolt.db", 0777, bolt.DefaultOptions)
		y.Check(err)
		boltdb.NoSync = !*syncWrite // Set this to speed up writes
		err = boltdb.Update(func(txn *bolt.Tx) error {
			var err error
			_, err = txn.CreateBucketIfNotExists([]byte("bench"))
			return err
		})
		y.Check(err)

	} else if *which == "leveldb" {
		init = true
		fmt.Println("Init LevelDB")
		if !*a {
			os.RemoveAll(*dir + "/level")
			os.MkdirAll(*dir+"/level", 0777)
		}
		ldb, err = leveldb.OpenFile(*dir+"/level/l.db", &opt.Options{
			NoSync: !*syncWrite,
		})
		y.Check(err)

	} else if *which == "lmdb" {
		init = true
		fmt.Println("Init lmdb")
		start := time.Now()
		o := lmdb.NoReadahead|lmdb.NoMemInit
		if !*syncWrite {
			o |= lmdb.NoSync
		}
		if !*syncMeta {
			o |= lmdb.NoMetaSync
		}
		var err error
		var dbopt = lmdb.DupSort|lmdb.DupFixed|lmdb.Create
		if *intdup {
			dbopt |= MDB_INTEGERDUP
		}
		var openlmdb = func(path string, i int) (env *lmdb.Env, dbi lmdb.DBI, dbih lmdb.DBI) {
			if !*a {
				os.RemoveAll(path)
			}
			os.MkdirAll(path, 0777)
			env, err = lmdb.NewEnv()
			env.SetMaxDBs(2)
			env.SetMapSize(int64(1 << 37))
			env.Open(path, uint(o), 0777)
			err = env.Update(func(txn *lmdb.Txn) error {
				dbi, err = txn.OpenDBI("bench", uint(dbopt))
				if err != nil {
					return err
				}
				dbih, err = txn.OpenDBI("bench-history", uint(dbopt))
				if err != nil {
					return err
				}
				return nil
			})
			y.Check(err)
			return
		}
		for i := 0; i < *n; i++ {
			for j := 0; j < *subparts; j++ {
				s := i * *subparts + j
				path := fmt.Sprintf(*dir+"/%x/lmdb/%x%x", i, i, j)
				lmdbEnvs[s], lmdbDBIs[s], lmdbDBIHistories[s] = openlmdb(path, s)
			}
		}
		fmt.Printf("Initialized after %v\n", time.Now().Sub(start))
	} else {
		log.Fatalf("Invalid value for option kv: '%s'", *which)
	}
	if len(*ldbhist) > 0 {
		init = true
		fmt.Println("Init LevelDB")
		var openlvldb = func(path string, i int) {
			if !*a {
				os.RemoveAll(path)
			}
			os.MkdirAll(path, 0777)
			ldbhistories[i], err = leveldb.OpenFile(path+"/level.db", nil)
			y.Check(err)
		}
		for i := 0; i < *n; i++ {
			for j := 0; j < *subparts; j++ {
				s := i * *subparts + j
				openlvldb(fmt.Sprintf(*ldbhist+"/%x/history/%x%x", i, i, j), s)
			}
		}
		y.Check(err)
	}

	if !init {
		log.Fatalf("Invalid arguments. Unable to init any store.")
	}
	rc := ratecounter.NewRateCounter(interval * time.Second)
	var counter int64
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		var count int64
		t := time.NewTicker(interval * time.Second)
		defer t.Stop()
		report := func() {
			// c := atomic.LoadInt64(&counter)
			kps := rc.Rate()
			kpsd := kps
			if kpsd < 1 {
				kpsd = 1
			}
			var dataItems int64
			var depth int64
			var branchPages int64
			var leafPages int64
			var overflowPages int64
			for i, env := range lmdbEnvs {
				env.View(func(txn *lmdb.Txn) error {
					stat, _ := txn.Stat(lmdbDBIs[i])
					dataItems += int64(stat.Entries)
					depth += int64(stat.Depth)
					branchPages += int64(stat.BranchPages)
					leafPages += int64(stat.LeafPages)
					overflowPages += int64(stat.OverflowPages)
					return nil
				})
			}
			// fmt.Printf(`
// [%04d] %s Per Second %s Total %s Deleted Scan: %s Rate: %s
               // Data Items:  Depth:  Branch Pages:  Leaf Pages:  Overflow Pages:
               // %s      %s      %s         %s        %s`,
				// count,
				// humanize(kps/int64(interval)),
				// humanize(c),
				// humanize(deletions.Rate()/int64(interval)),
				// humanize(scan.Rate()/int64(interval)),
				// humanizeFloat(float64(scan.Rate())/float64(kpsd)),
				// humanize(dataItems),
				// humanizeFloat(float64(depth) / float64(len(lmdbEnvs))),
				// humanize(branchPages / int64(len(lmdbEnvs))),
				// humanize(leafPages / int64(len(lmdbEnvs))),
				// humanize(overflowPages / int64(len(lmdbEnvs))))
			fmt.Printf("%d,%d,%d,%d,%d,%d\n",
				dataItems,
				kps,
				int64(scan.Rate()),
				int64(depth) / int64(len(lmdbEnvs)),
				int64(branchPages) / int64(len(lmdbEnvs)),
				int64(leafPages) / int64(len(lmdbEnvs)))
			count++
		}
		var headers bool
		for {
			select {
			case <-t.C:
				if !headers {
					fmt.Printf("total,rate,scans,depth,branchnodes,leafnodes\n")
					headers = true
				}
				report()
			case <-ctx.Done():
				report()
				return
			}
		}
	}()
	go func() {
		if err := http.ListenAndServe("0.0.0.0:8081", nil); err != nil {
			log.Fatalf("While opening http. Error: %v", err)
		}
	}()

	N = *n * *subparts
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(proc int) {
			entries := make([]*entry, *batchSize)
			for i := 0; i < len(entries); i++ {
				e := new(entry)
				if *seqKeys {
					e.Key = make([]byte, 8)
				} else {
					e.Key = make([]byte, *keySize)
				}
				e.Value = make([]byte, *valueSize)
				entries[i] = e
			}
			var written float64
			var i int
			for written < nw/float64(N) {
				wrote := float64(writeBatch(entries, i, proc))

				wi := int64(wrote)
				atomic.AddInt64(&counter, wi)

				rc.Incr(wi)

				written += wrote
				i++
			}
			wg.Done()
		}(i)
	}
	// 	wg.Add(1) // Block
	wg.Wait()
	cancel()

	if bdb != nil {
		fmt.Println("closing badger")
		bdb.Close()
	}

	if rdb != nil {
		fmt.Println("closing rocks")
		rdb.Close()
	}

	if ldb != nil {
		fmt.Println("closing leveldb")
		ldb.Close()
	}

	if boltdb != nil {
		fmt.Println("closing bolt")
		boltdb.Close()
	}

	if lmdbEnv != nil {
		fmt.Println("closing lmdb")
		lmdbEnv.CloseDBI(lmdbDBI)
		lmdbEnv.CloseDBI(lmdbDBIHistory)
		lmdbEnv.Close()
	}

	fmt.Printf("\nWROTE %d KEYS\n", atomic.LoadInt64(&counter))
}
