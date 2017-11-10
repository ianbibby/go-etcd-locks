package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

var (
	requestTimeout = 10 * time.Second
	dialTimeout    = 10 * time.Second
	servers        string
	locks          string
	duration       int
)

func main() {
	parseArguments()

	var ctx context.Context
	var cancel context.CancelFunc

	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints:   strings.Split(servers, ","),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	session, err := concurrency.NewSession(cli, concurrency.WithTTL(120))
	if err != nil {
		log.Fatal(err)
	}

	var mutexes []*concurrency.Mutex
	sortedLocks := strings.Split(locks, ",")
	sort.Strings(sortedLocks)

	// Acquire locks in strict order.
	for _, lock := range sortedLocks {
		mutex := concurrency.NewMutex(session, "/"+lock)
		ctx, cancel = makeBGCtx()
		if err = mutex.Lock(ctx); err != nil {
			log.Fatal(err)
		}
		cancel()
		mutexes = append(mutexes, mutex)
	}

	log.Println("START job")
	time.Sleep(time.Duration(duration) * time.Second)
	log.Println("FINISH job")

	// Release locks in reverse order.
	for i := len(mutexes) - 1; i >= 0; i-- {
		ctx, cancel = makeBGCtx()
		mutexes[i].Unlock(ctx)
		cancel()
	}

	session.Close()
}

func parseArguments() {
	flag.StringVar(&servers, "server", "localhost:2379", "etcd server")
	flag.StringVar(&locks, "locks", "", "resources to lock, comma-separated")
	flag.IntVar(&duration, "duration", 5, "job duration (unit: seconds)")
	flag.Parse()

	if len(locks) == 0 {
		fmt.Fprintf(os.Stderr, "Locks not specified\n")
		os.Exit(2)
	}
}

func makeBGCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(),
		time.Duration(duration)*time.Second)
}
