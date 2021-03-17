package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	workers        = 0
	iterations     = 0
	flagBenchmarks *string
	benchmarks     []string

	flagMetricsPort *string

	flagCluster        *string
	flagProto          *int
	flagCQL            *string
	flagRF             *int
	flagRetry          *int
	flagCompressTest   *string
	flagTimeout        *time.Duration
	flagCreateKeyspace *bool
	flagKeyspace       *string
	flagNumConn        *int
	flagUsername       *string
	flagPassword       *string
	clusterHosts       []string

	totalInsertIterationsVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_insert_iterations",
			Help:      "Amount of Inserts Per Benchmark",
		},
		[]string{"benchmark_id", "type"},
	)
	totalInsertTimeVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_insert_time",
			Help:      "Inserts Time Per Iteration",
		},
		[]string{"benchmark_id", "type"},
	)

	totalGetIterationsVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_get_iterations",
			Help:      "Amount of Gets Per Benchmark",
		},
		[]string{"benchmark_id", "type"},
	)
	totalGetTimeVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_get_time",
			Help:      "Gets Time Per Iteration",
		},
		[]string{"benchmark_id", "type"},
	)

	totalSelectIterationsVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_select_iterations",
			Help:      "Amount of Selects Per Benchmark",
		},
		[]string{"benchmark_id", "type"},
	)
	totalSelectTimeVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "total_select_time",
			Help:      "Selects Time Per Iteration",
		},
		[]string{"benchmark_id", "type"},
	)

	serviceLevelsVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "worker",
			Subsystem: "benchmarks",
			Name:      "service_levels",
			Help:      "The service level shares for workloads",
		},
		[]string{"type"},
	)
)

type bench func(*testing.B)

// Benchmark struct
type Benchmark struct {
	Type      string
	Benchmark bench
}

var benchPersonSchema string

func init() {
	flag.IntVar(&workers, "workers", 10, "Number of workers to use")
	flag.IntVar(&iterations, "iterations", 500, "Number of iterations per benchmark")
	flagMetricsPort = flag.String("port", "9009", "a port number where the prometheus metric server will be running on")
	flagBenchmarks = flag.String("benchmarks", "insert,get,select", "a comma-separated list of benchmarks to run [insert,get,select]")
	flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples")
	flagProto = flag.Int("proto", 0, "protcol version")
	flagCQL = flag.String("cql", "3.0.0", "CQL version")
	flagRF = flag.Int("rf", 1, "replication factor for test keyspace")
	flagRetry = flag.Int("retries", 5, "number of times to retry queries")
	flagCompressTest = flag.String("compressor", "", "compressor to use")
	flagTimeout = flag.Duration("gocql.timeout", 5*time.Second, "sets the connection `timeout` for all operations")
	flagCreateKeyspace = flag.Bool("fresh", false, "sets if the benchmarks should start fresh by dropping the keyspace")
	flagKeyspace = flag.String("keyspace", "workload", "name of the keyspace to create")
	flagNumConn = flag.Int("connections", 2, "number of connections per host")
	flagUsername = flag.String("username", "", "the username to use for auth enabled clusters")
	flagPassword = flag.String("password", "", "the password to use for auth enabled clusters")
}

func trackBenchmark(bench bench, b *testing.B) testing.BenchmarkResult {
	start := time.Now()
	bench(b)
	duration := time.Since(start)
	return testing.BenchmarkResult{T: duration, N: iterations}
}

func makeBenchmark() *Benchmark {
	t := benchmarks[rand.Int()%len(benchmarks)]
	b := &Benchmark{
		Type:      t,
		Benchmark: benchmarks.BenchmarkE2EGocqlxInsert,
	}
	switch t {
	case "insert":
		b.Benchmark = BenchmarkE2EGocqlxInsert
	case "get":
		b.Benchmark = BenchmarkE2EGocqlxGet
	case "select":
		b.Benchmark = BenchmarkE2EGocqlxSelect
	}
	return b
}

func createBenchmarks(benchmarks chan<- *Benchmark) {
	log.Printf("[INFO] starting benchmark maker\n")
	for {
		b := makeBenchmark()
		benchmarks <- b
		time.Sleep(5 * time.Millisecond)
	}
}

func startBenchmarkProcessor(benchmarks <-chan *Benchmark) {
	log.Printf("[INFO] starting %d workers\n", workers)

	bt := &testing.B{
		N: iterations,
	}

	//log.Printf("[INFO] loading data into memory\n")
	//people = loadFixtures()

	if err := session.ExecStmt(session, benchPersonSchema); err != nil {
		log.Println(err)
	}

	wait := sync.WaitGroup{}
	// notify the sync group we need to wait for N goroutines
	wait.Add(workers)

	// start N workers
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			// start the worker
			startWorker(workerID, bt, benchmarks)
			wait.Done()
		}(i)
	}
	log.Printf("[INFO] started workers\n")

	wait.Wait()
}

func startWorker(workerID int, b *testing.B, benchmarks <-chan *Benchmark) {
	for {
		select {
		// read from the channel
		case benchmark := <-benchmarks:
			log.Printf("Starting Benchmark %v+\n", benchmark)
			// b := testing.Benchmark(benchmark.Benchmark)
			tb := trackBenchmark(benchmark.Benchmark, b)
			log.Printf("%v+\n", tb)
			log.Println("N of iters:", tb.N)
			log.Println("Ns per op:", tb.NsPerOp())
			log.Println("Time per op:", time.Duration(tb.NsPerOp()))
			switch benchmark.Type {
			case "insert":
				totalInsertIterationsVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxInsert_", workerID), "insert").Observe(float64(tb.N))
				totalInsertTimeVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxInsertTime_", workerID), "insert").Observe(float64(tb.NsPerOp() / 1000000))
			case "get":
				totalGetIterationsVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxGet_", workerID), "get").Observe(float64(tb.N))
				totalGetTimeVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxGetTime_", workerID), "get").Observe(float64(tb.NsPerOp() / 1000000))
			case "select":
				totalSelectIterationsVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxSelect_", workerID), "select").Observe(float64(tb.N))
				totalSelectTimeVec.WithLabelValues(fmt.Sprint("BenchmarkE2EGocqlxSelectTime_", workerID), "select").Observe(float64(tb.NsPerOp() / 1000000))
			}
		}
	}
}

func queryWorkloadPrioritization() {
	nextTime := time.Now().Truncate(time.Second * 10)
	var serviceLevel string
	var shares int
	for {
		nextTime = nextTime.Add(time.Second * 10)
		time.Sleep(time.Until(nextTime))
		iter := session.Query("LIST ALL SERVICE_LEVELS").Iter()
		for iter.Scan(&serviceLevel, &shares) {
			fmt.Printf("Service Item, %s : %v\n", serviceLevel, shares)
			serviceLevelsVec.WithLabelValues(serviceLevel).Set(float64(shares))
		}
		if err := iter.Close(); err != nil {
			log.Println(err)
		}
	}
}

func main() {
	// parse the flags
	flag.Parse()
	benchmarks = strings.Split(*flagBenchmarks, ",")
	clusterHosts = strings.Split(*flagCluster, ",")
	benchPersonSchema = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.bench_person (
			id int,
			first_name text,
			last_name text,
			email list<text>,
			gender text,
			ip_address text,
			PRIMARY KEY(id)
		)`, *flagKeyspace)

	fmt.Printf("Benchmarks to run: %s\n", benchmarks)

	// register with the prometheus collector
	prometheus.MustRegister(
		totalInsertIterationsVec,
		totalInsertTimeVec,
		totalGetIterationsVec,
		totalGetTimeVec,
		totalSelectIterationsVec,
		totalSelectTimeVec,
		serviceLevelsVec,
	)

	bt := &testing.B{
		N: iterations,
	}

	log.Printf("[INFO] creating session and keyspaces\n")
	session = session.CreateSession(clusterHosts, false, bt)

	benchmarkChannel := make(chan *Benchmark, 10000)

	// workers that will process benchmarks
	go startBenchmarkProcessor(benchmarkChannel)

	// start benchmarks
	go createBenchmarks(benchmarkChannel)

	// query for workload shares
	go queryWorkloadPrioritization()

	handler := http.NewServeMux()
	handler.Handle("/metrics", promhttp.Handler())

	log.Printf("[INFO] starting HTTP server on port :%s", *flagMetricsPort)
	log.Fatal(http.ListenAndServe(":"+*flagMetricsPort, handler))
}
