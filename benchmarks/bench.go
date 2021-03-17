package benchmarks

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/6sack/gocqlx"
	"github.com/6sack/gocqlx/qb"
)

type benchPerson struct {
	ID        int      `json:"id"`
	FirstName string   `json:"first_name"`
	LastName  string   `json:"last_name"`
	Email     []string `json:"email"`
	Gender    string   `json:"gender"`
	IPAddress string   `json:"ip_address"`
}

var (
	people          []*benchPerson
	benchPersonCols = []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}
)

// BenchmarkE2EGocqlxSelect performs select.
func BenchmarkE2EGocqlxSelect(b *testing.B) {
	log.Printf("starting\n")

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

	stmt, names := qb.Insert(*flagKeyspace + ".bench_person").Columns(benchPersonCols...).ToCql()
	q := gocqlx.Query(session.Query(stmt), names)
	defer q.Release()

	for i := 0; i < b.N; i++ {
		rand.Seed(time.Now().UnixNano())
		p := people[i%len(people)]
		p.ID = rand.Intn(1000000-1+1) + 1
		if err := q.BindStruct(p).Exec(); err != nil {
			log.Printf("Error: %s", err)
			time.Sleep(100 * time.Millisecond)
		}
	}
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
