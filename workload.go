package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/6sack/gocqlx/qb"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/gocqlx"
)

var (
	workers        = 0
	iterations     = 0
	flagBenchmarks *string
	benchmarks     []string
	people         []*benchPerson
	session        *gocql.Session

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

type benchPerson struct {
	ID        int      `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     []string `json:"email"`
	Gender    string   `json:"gender"`
	IPAddress string   `json:"ip_address"`
}

type bench func(*testing.B)

// Benchmark struct
type Benchmark struct {
	Type      string
	Benchmark bench
}

var benchPersonSchema string

var benchPersonCols = []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}

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

func loadFixtures() []*benchPerson {
	f, err := os.Open("testdata/people.json")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}()

	var v []*benchPerson
	if err := json.NewDecoder(f).Decode(&v); err != nil {
		panic(err)
	}

	return v
}

func initTable(b *testing.B, session *gocql.Session, people []*benchPerson) {
	if err := ExecStmt(session, benchPersonSchema); err != nil {
		log.Println(err)
	}

	stmt, names := qb.Insert(*flagKeyspace + ".bench_person").Columns(benchPersonCols...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)

	for _, p := range people {
		if err := q.BindStruct(p).Exec(); err != nil {
			log.Println(err)
		}
	}
}

// BenchmarkE2EGocqlxSelect performs select.
func BenchmarkE2EGocqlxSelect(b *testing.B) {
	log.Printf("starting\n")
	//defer session.Close()

	//initTable(b, session, people)

	stmt, _ := qb.Select(*flagKeyspace + ".bench_person").Columns(benchPersonCols...).Limit(100).ToCql()

	for i := 0; i < b.N; i++ {
		// prepare
		q := session.Query(stmt)
		var v []*benchPerson
		// select and release
		if err := gocqlx.Query(q, nil).SelectRelease(&v); err != nil {
			log.Printf("Error: %s", err)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// BenchmarkE2EGocqlxGet performs get.
func BenchmarkE2EGocqlxGet(b *testing.B) {
	log.Printf("starting\n")
	//defer session.Close()

	//initTable(b, session, people)

	stmt, _ := qb.Select(*flagKeyspace + ".bench_person").Columns(benchPersonCols...).Where(qb.Eq("id")).Limit(1).ToCql()
	var p benchPerson

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// prepare
		rand.Seed(time.Now().UnixNano())
		q := session.Query(stmt).Bind(rand.Intn(1000000-1+1) + 1)
		//q := session.Query(stmt).Bind(people[i%len(people)].ID)
		// get and release
		if err := gocqlx.Query(q, nil).GetRelease(&p); err != nil {
			log.Printf("Error: %s", err)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// BenchmarkE2EGocqlxInsert performs insert with struct binding.
func BenchmarkE2EGocqlxInsert(b *testing.B) {
	log.Printf("starting\n")
	//defer session.Close()

	stmt, names := qb.Insert(*flagKeyspace + ".bench_person").Columns(benchPersonCols...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	defer q.Release()

	for i := 0; i < b.N; i++ {
		rand.Seed(time.Now().UnixNano())
		p := people[i%len(people)]
		p.ID = rand.Intn(1000000-1+1) + 1
		//fmt.Println(p)
		if err := q.BindStruct(p).Exec(); err != nil {
			log.Printf("Error: %s", err)
			time.Sleep(100 * time.Millisecond)
		}
	}
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
		Benchmark: BenchmarkE2EGocqlxInsert,
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

	log.Printf("[INFO] loading data into memory\n")
	people = loadFixtures()

	if err := ExecStmt(session, benchPersonSchema); err != nil {
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

var initOnce sync.Once

// CreateSession creates a new gocql session from flags.
func CreateSession(tb testing.TB) *gocql.Session {
	cluster := createCluster()
	return createSessionFromCluster(cluster, tb)
}

func createCluster() *gocql.ClusterConfig {
	fmt.Printf("trying to connect to cluster hosts: %s", clusterHosts)
	cluster := gocql.NewCluster(clusterHosts...)
	if len(*flagUsername) > 0 && len(*flagPassword) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *flagUsername,
			Password: *flagPassword,
		}
	}
	cluster.ProtoVersion = *flagProto
	cluster.CQLVersion = *flagCQL
	cluster.Timeout = *flagTimeout
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = *flagNumConn
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if *flagRetry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *flagRetry}
	}

	switch *flagCompressTest {
	case "snappy":
		cluster.Compressor = &gocql.SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + *flagCompressTest)
	}

	return cluster
}

func createSessionFromCluster(cluster *gocql.ClusterConfig, tb testing.TB) *gocql.Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		if *flagCreateKeyspace == true {
			createKeyspace(tb, cluster, *flagKeyspace)
		}
	})

	cluster.Keyspace = *flagKeyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println("CreateSession:", err)
	}

	return session
}

func createKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string) {
	log.Printf("[INFO] recreating keyspace: %s with replication of %d", keyspace, *flagRF)
	c := *cluster
	c.Keyspace = "system"
	c.Timeout = 30 * time.Second
	session, err := c.CreateSession()
	if err != nil {
		log.Println(err)
	}
	defer session.Close()

	err = ExecStmt(session, `DROP KEYSPACE IF EXISTS `+keyspace)
	if err != nil {
		log.Printf("unable to drop keyspace: %v", err)
	}

	err = ExecStmt(session, fmt.Sprintf(`CREATE KEYSPACE %s
	WITH replication = {
		'class' : 'SimpleStrategy',
		'replication_factor' : %d
	}`, keyspace, *flagRF))

	if err != nil {
		log.Printf("unable to create keyspace: %v", err)
	}
}

// ExecStmt executes a statement string.
func ExecStmt(s *gocql.Session, stmt string) error {
	q := s.Query(stmt).RetryPolicy(nil)
	defer q.Release()
	return q.Exec()
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
	session = CreateSession(bt)

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
