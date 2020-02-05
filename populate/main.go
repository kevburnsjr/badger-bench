package main

import (
	"context"
	"flag"
	"fmt"
	"bytes"
	"log"
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

var (
	which     = flag.String("kv", "", "Which KV store to use. Options: badger, rocksdb, bolt, leveldb, lmdb")
	numKeys   = flag.Float64("keys_mil", 10.0, "How many million keys to write.")
	valueSize = flag.Int("valsz", 128, "Value size in bytes.")
	keySize   = flag.Int("keysz", 16, "Key size in bytes.")
	dir       = flag.String("dir", "", "Base dir for writes.")
	mode      = flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")
	withRead  = flag.Bool("read", false, "Read each key prior to write.")
	keypfx    = flag.Int("keypfx", 4, "Bucket key size")
	keytrunc  = flag.Int("keytrunc", 0, "Truncate n bits from bucket key")
	readOther = flag.Bool("read-other", false, "Read an unrelated key prior to each write.")
	seqKeys   = flag.Bool("keys_seq", false, "Use sequential keys.")
	history   = flag.Bool("history", false, "Store history.")
	batchSize = flag.Int("batchsize", 1000, "Batch Size")
	tsonly    = flag.Bool("tsonly", false, "Store timeseries only")
	syncWrite = flag.Bool("sync", false, "Strong durability")
	syncEvery = flag.Int("sync-every", 0, "Number of batches after which to force sync")
	n         = flag.Int("n", 32, "Number of concurrent writers")
	a         = flag.Bool("a", false, "Do not delete existing database (append)")
	partition = flag.Bool("p", false, "Partition per writer")
	subparts  = flag.Int("sp", 1, "Subpartitions per writer")
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

func fillEntry(e *entry, workerNum int) {
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
		rand.Read(e.Key)
	}
	if *n > 0 {
		e.Key[0] = byte(workerNum+1)
	}
	for i := 0; i < *keytrunc; i++ {
		e.Key[*keypfx-1] = e.Key[*keypfx-1] &^ byte(2^i)
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
var lmdbEnv *lmdb.Env
var lmdbDBI lmdb.DBI
var lmdbDBIHistory lmdb.DBI
var lmdbEnvs = map[int]*lmdb.Env{}
var lmdbDBIs = map[int]lmdb.DBI{}
var lmdbDBIHistories = map[int]lmdb.DBI{}
var avgLen int64
var scan = ratecounter.NewRateCounter(10 * time.Second)

func writeBatch(entries []*entry, batchNum, workerNum int) int {
	for _, e := range entries {
		fillEntry(e, workerNum)
	}
	var err error
	var keylen = len(entries[0].Key)
	var keys = make([]byte, len(entries) * keylen)

	var ts = make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixNano()))

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
		// start := time.Now()
		var lmdbDBI = lmdbDBI
		var lmdbDBIHistory = lmdbDBIHistory
		var scanned int64
		err := lmdbEnv.Update(func(txn *lmdb.Txn) error {
			var k = make ([]byte, *keypfx)
			var hist = map[int][]byte{}
			for _, e := range entries {
				sub := rand.Intn(*subparts)
				e.Key[0] = byte(workerNum * *n + sub)
				k = e.Key[0:*keypfx]
				lmdbDBI = lmdbDBIs[int(e.Key[0])]
				lmdbDBIHistory = lmdbDBIHistories[int(e.Key[0])]
				tail := e.Key[*keypfx:]
				v, err := txn.Get(lmdbDBI, k)
				if !lmdb.IsNotFound(err) {
					y.Check(err)
				}
				var duplicate = false
				for j := 0; j < len(v); j += subkeylen {
					scanned++
					if bytes.Equal(v[j:j+(subkeylen)], tail) {
						duplicate = true
						break
					}
				}
				if duplicate {
					println("duplicate")
					continue
				}
				v = append(v, tail...)
				if !*tsonly {
					err := txn.Put(lmdbDBI, k, v, 0)
					if err != nil {
						return err
					}
				}
				if _, ok := hist[sub]; !ok {
					hist[sub] = e.Key
				} else {
					hist[sub] = append(hist[sub], e.Key...)
				}
			}
			for i, keys := range hist {
				if *history || *tsonly {
					if err := txn.Put(lmdbDBIHistories[i], ts, keys, 0); err != nil {
						return err
					}
				}
				if err := txn.Put(lmdbDBIs[i], []byte("checkpoint"), ts, 0); err != nil {
					return err
				}
			}
			return nil
		})
		if *syncEvery > 0 && batchNum % *syncEvery == 0 {
			lmdbEnv.Sync(true)
		}
		y.Check(err)
		scan.Incr(scanned)
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
		o := lmdb.NoReadahead
		if !*syncWrite {
			o |= lmdb.NoSync
		}
		var openlmdb = func(path string, i int) (env *lmdb.Env) {
			if !*a {
				os.RemoveAll(path)
			}
			os.MkdirAll(path, 0777)
			env, err := lmdb.NewEnv()
			y.Check(err)
			err = env.SetMaxDBs(32)
			y.Check(err)
			err = env.SetMapSize(1 << 38) // ~273Gb
			y.Check(err)
			err = env.Open(path, uint(o), 0777)
			y.Check(err)
			// Acquire handle
			err = env.Update(func(txn *lmdb.Txn) error {
				var err error
				for j := 0; j < *subparts; j++ {
					s := i * *n + j
					lmdbDBIs[s], err = txn.CreateDBI(fmt.Sprintf("bench-%d", s))
					if err != nil {
						y.Check(err)
					}
					lmdbDBIHistories[s], err = txn.CreateDBI(fmt.Sprintf("bench-%d-history", s))
					if err != nil {
						y.Check(err)
					}
				}
				return err
			})
			y.Check(err)
			return
		}
		if *partition {
			for i := 0; i < *n; i++ {
				path := fmt.Sprintf(*dir+"/%x/lmdb", i)
				lmdbEnvs[i] = openlmdb(path, i)
			}
		} else {
			lmdbEnv = openlmdb(*dir + "/lmdb", 0)
			lmdbDBI = lmdbDBIs[0]
			lmdbDBIHistory = lmdbDBIHistories[0]
		}
		fmt.Printf("Initialized after %v\n", time.Now().Sub(start))
	} else {
		log.Fatalf("Invalid value for option kv: '%s'", *which)
	}

	if !init {
		log.Fatalf("Invalid arguments. Unable to init any store.")
	}

	rc := ratecounter.NewRateCounter(10 * time.Second)
	var counter int64
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		var count int64
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				fmt.Printf("[%04d] Write key rate per second: %s. Total: %s    Scan Rate: %s\n",
					count,
					humanize(rc.Rate()/10),
					humanize(atomic.LoadInt64(&counter)),
					humanize(scan.Rate()/10))
				count++
			case <-ctx.Done():
				fmt.Printf("[%04d] Write key rate per second: %s. Total: %s    Scan Rate: %s\n",
					count,
					humanize(rc.Rate()/10),
					humanize(atomic.LoadInt64(&counter)),
					humanize(scan.Rate()/10))
				return
			}
		}
	}()
	go func() {
		if err := http.ListenAndServe("0.0.0.0:8081", nil); err != nil {
			log.Fatalf("While opening http. Error: %v", err)
		}
	}()

	N := *n
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
				i++
				wrote := float64(writeBatch(entries, i, proc))

				wi := int64(wrote)
				atomic.AddInt64(&counter, wi)

				rc.Incr(wi)

				written += wrote
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
