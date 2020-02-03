package main

import (
	"context"
	"flag"
	"fmt"
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
	readOther = flag.Bool("read-other", false, "Read an unrelated key prior to each write.")
	seqKeys   = flag.Bool("keys_seq", false, "Use sequential keys.")
	history   = flag.Bool("history", false, "Store history.")
	batchSize = flag.Int("batchsize", 1000, "Batch Size")
	tsonly    = flag.Bool("tsonly", false, "Store timeseries only")
	syncWrite = flag.Bool("sync", false, "Strong durability")
	syncEvery = flag.Int("sync-every", 0, "Number of batches after which to force sync")
	n         = flag.Int("n", 32, "Number of concurrent writers")
	a         = flag.Bool("a", false, "Do not delete existing database (append)")
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

func fillEntry(e *entry) {
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
	rand.Read(e.Value)
	e.Meta = 0
}

var bdb *badger.DB

var rdb *store.Store
var boltdb *bolt.DB
var ldb *leveldb.DB
var lmdbEnv *lmdb.Env
var lmdbDBI lmdb.DBI
var lmdbDBIHistory lmdb.DBI

func writeBatch(entries []*entry, batchNum int) int {
	for _, e := range entries {
		fillEntry(e)
	}
	var err error
	var keylen = len(entries[0].Key)
	var keys = make([]byte, len(entries) * keylen)

	var ts = make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixNano()))

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
		rb := rdb.NewWriteBatch()
		defer rb.Destroy()
		for i, e := range entries {
			if *withRead {
				k := e.Key
				if *readOther {
					k = []byte(Reverse(string(k)))
				}
				v, _ := rdb.Get(k)
				if v.Size() > 0 {
					println("duplicate")
					continue
				}
			}
			if !*tsonly {
				rb.Put(e.Key, e.Value)
			}
			copy(keys[i*keylen:i*keylen+keylen], e.Key)
		}
		if *history || *tsonly {
			rb.Put(ts, keys)
		}
		y.Check(rdb.WriteBatch(rb))
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
		err := lmdbEnv.Update(func(txn *lmdb.Txn) error {
			for i, e := range entries {
				if *withRead {
					k := e.Key
					if *readOther {
						k = []byte(Reverse(string(k)))
					}
					_, err = txn.Get(lmdbDBI, k)
					if err == nil {
						println("duplicate")
						continue
					}
				}
				if !*tsonly {
					err := txn.Put(lmdbDBI, e.Key, e.Value, 0)
					if err != nil {
						return err
					}
				}
				copy(keys[i*keylen:i*keylen+keylen], e.Key)
			}
			if *history || *tsonly {
				if err := txn.Put(lmdbDBIHistory, ts, keys, 0); err != nil {
					return err
				}
			}
			return nil
		})
		if *syncEvery > 0 && batchNum % *syncEvery == 0 {
			println("sync")
			lmdbEnv.Sync(true)
		}
		y.Check(err)
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
		if !*a {
			os.RemoveAll(*dir + "/lmdb")
			os.MkdirAll(*dir+"/lmdb", 0777)
		}

		lmdbEnv, err = lmdb.NewEnv()
		y.Check(err)
		err = lmdbEnv.SetMaxDBs(2)
		y.Check(err)
		err = lmdbEnv.SetMapSize(1 << 38) // ~273Gb
		y.Check(err)
		o := lmdb.NoReadahead
		if !*syncWrite {
			o |= lmdb.NoSync
		}
		err = lmdbEnv.Open(*dir+"/lmdb", uint(o), 0777)
		y.Check(err)

		// Acquire handle
		err := lmdbEnv.Update(func(txn *lmdb.Txn) error {
			var err error
			lmdbDBI, err = txn.CreateDBI("bench")
			if err != nil {
				return nil
			}
			lmdbDBIHistory, err = txn.CreateDBI("bench-history")
			return err
		})
		y.Check(err)
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
				fmt.Printf("[%04d] Write key rate per second: %s. Total: %s\n",
					count,
					humanize(rc.Rate()/10),
					humanize(atomic.LoadInt64(&counter)))
				count++
			case <-ctx.Done():
				fmt.Printf("[%04d] Write key rate per second: %s. Total: %s\n",
					count,
					humanize(rc.Rate()/10),
					humanize(atomic.LoadInt64(&counter)))
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
			for written < nw/float64(N) {
				wrote := float64(writeBatch(entries, int(written / float64(*batchSize))))

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
		lmdbEnv.Close()
	}

	fmt.Printf("\nWROTE %d KEYS\n", atomic.LoadInt64(&counter))
}
