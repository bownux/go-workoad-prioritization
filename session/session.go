package session

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

var (
	session *gocql.Session
)

var initOnce sync.Once

//CassandraSession session
type CassandraSession struct {
	ClusterHosts []string
	Keyspace     string
	Username     string
	Password     string
	ProtoVersion int
	Version      string
	Timeout      time.Duration
	NumConn      int
	Retry        int
	Compressor   string
}

// CreateSession creates a new gocql session from flags.
func (cs *CassandraSession) CreateSession(clusterHosts []string, fresh bool, tb testing.TB) *gocql.Session {
	cluster := createCluster(clusterHosts, "cassandra", "cassandra")
	return createSessionFromCluster(cluster, fresh, cs.Keyspace, tb)
}

func createCluster(clusterHosts []string, username string, password string, protocol int, version string, timeout time.Duration, numConn int, retry int, compressor string) *gocql.ClusterConfig {
	fmt.Printf("trying to connect to cluster hosts: %s", clusterHosts)
	cluster := gocql.NewCluster(clusterHosts...)
	if len(username) > 0 && len(password) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	cluster.ProtoVersion = protocol
	cluster.CQLVersion = version
	cluster.Timeout = timeout
	cluster.Consistency = gocql.Quorum
	cluster.NumConns = numConn
	cluster.MaxWaitSchemaAgreement = 2 * time.Minute // travis might be slow
	if retry > 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: retry}
	}

	switch compressor {
	case "snappy":
		cluster.Compressor = &gocql.SnappyCompressor{}
	case "":
	default:
		panic("invalid compressor: " + compressor)
	}

	return cluster
}

func createSessionFromCluster(cluster *gocql.ClusterConfig, fresh bool, keyspace string, tb testing.TB) *gocql.Session {
	// Drop and re-create the keyspace once. Different tests should use their own
	// individual tables, but can assume that the table does not exist before.
	initOnce.Do(func() {
		if fresh == true {
			createKeyspace(tb, cluster, keyspace)
		}
	})

	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		log.Println("CreateSession:", err)
	}

	return session
}

func createKeyspace(tb testing.TB, cluster *gocql.ClusterConfig, keyspace string, rf string) {
	log.Printf("[INFO] recreating keyspace: %s with replication of %d", keyspace, rf)
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
	}`, keyspace, rf))

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
