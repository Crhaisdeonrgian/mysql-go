package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/KyleBanks/dockerstats"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"gonum.org/v1/plot/plotter"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/tabwriter"
	"time"
)

type queryComplexity int

const rowsCount = 400000
const iterationCount = 100

var fakeRows *sql.Rows

const (
	SimpleComlQuery queryComplexity = 0
	MediumComlQuery                 = 1
	HardComlQuery                   = 2
)

const (
	SimpleQuery string = "select * from abobd where aa"
	MediumQuery        = "select * from abobd where o<110000 order by bb desc, aa asc"
	HardQuery          = "select * from abobd first join abobd second on second.o<5 where first.aa like '%a%' order by first.bb desc, first.aa asc"
)

const (
	IgorMountPoint string = "/Users/igorvozhga/DIPLOMA/mountDir:/var/lib/mysql"
	MikeMountPoint        = "/home/user/go/mounts:/var/lib/mysql"
)

var driverName string

func init() {
	driverName = "mysqlc"
	CancelModeUsage = true
	DebugMode = false
}

type mySQLProcInfo struct {
	ID      int64   `db:"Id"`
	User    string  `db:"User"`
	Host    string  `db:"Host"`
	DB      string  `db:"db"`
	Command string  `db:"Command"`
	Time    int     `db:"Time"`
	State   string  `db:"State"`
	Info    *string `db:"Info"`
}

// nolint:gochecknoglobals
var dockerPool *dockertest.Pool // the connection to docker
// nolint:gochecknoglobals
var systemdb *sql.DB // the connection to the mysql 'system' database
// nolint:gochecknoglobals
var sqlConfig *mysql.Config // the mysql container and config for connecting to other databases
// nolint:gochecknoglobals
var testMu *sync.Mutex // controls access to sqlConfig

type mySQLProcsInfo []mySQLProcInfo

func TestBench(t *testing.T) {
	simplebanch(1, foo)
}
func foo() {
	var complexity queryComplexity
	complexity = HardComlQuery
	var err error
	var dbStd *sql.DB
	testMu.Lock()
	benchTestConfig := sqlConfig
	testMu.Unlock()
	benchTestConfig.DBName = "BigBench"
	dbStd, err = sql.Open(driverName, // Cancelable driver instead of mysql
		benchTestConfig.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	queryctx, querycancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer querycancel()
	if complexity == HardComlQuery {
		_, err = dbStd.QueryContext(queryctx, HardQuery)
		//2-3 min
	} else if complexity == SimpleComlQuery {
		_, err = dbStd.QueryContext(queryctx, SimpleQuery)
		//0.014 sec
	} else if complexity == MediumComlQuery {
		_, err = dbStd.QueryContext(queryctx, MediumQuery)
		//5 sec
	}
	if err != context.DeadlineExceeded && err != nil {
		log.Fatal(err)
	}

}

func helperFullProcessList(db *sql.DB) (mySQLProcsInfo, error) {
	dbx := sqlx.NewDb(db, driverName)
	var procs []mySQLProcInfo
	if err := dbx.Select(&procs, "show full processlist"); err != nil {
		return nil, err
	}
	return procs, nil
}

func (ms mySQLProcsInfo) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "ID\tUser\tHost\tDB\tCommand\tTime\tState\tInfo")
	for _, m := range ms {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", m.ID, m.User, m.Host, m.DB, m.Command, m.Time,
			m.State, m.Info)
	}
	w.Flush()
	return buf.String()
}

func (ms mySQLProcsInfo) Filter(fns ...func(m mySQLProcInfo) bool) (result mySQLProcsInfo) {
	for _, m := range ms {
		ok := true
		for _, fn := range fns {
			if !fn(m) {
				ok = false
				break
			}
		}
		if ok {
			result = append(result, m)
		}
	}
	return result
}

func TestOptions(t *testing.T) {
	time.Sleep(30 * time.Second)
}
func TestMain(m *testing.M) {
	_ = mysql.SetLogger(log.New(ioutil.Discard, "", 0)) // silence mysql logger
	testMu = &sync.Mutex{}

	var err error
	dockerPool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s", err)
	}
	dockerPool.MaxWait = time.Minute * 2

	runOptions := dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5.6",
		Env:        []string{"MYSQL_ROOT_PASSWORD=secret"},
		Mounts:     []string{MikeMountPoint},
	}
	mysqlContainer, err := dockerPool.RunWithOptions(&runOptions, func(hostcfg *docker.HostConfig) {
		hostcfg.CPUCount = 1
		//hostcfg.CPUPercent = 100
		hostcfg.Memory = 1024 * 1024 * 1024 * 1 //1Gb
	})
	if err != nil {
		log.Fatalf("could not start mysqlContainer: %s", err)
	}
	sqlConfig = &mysql.Config{
		User:                 "root",
		Passwd:               "secret",
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("localhost:%s", mysqlContainer.GetPort("3306/tcp")),
		DBName:               "mysql",
		AllowNativePasswords: true,
	}

	if err = dockerPool.Retry(func() error {
		systemdb, err = sql.Open(driverName, sqlConfig.FormatDSN())
		if err != nil {
			return err
		}
		return systemdb.Ping()
	}); err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// You can't defer this because os.Exit ignores defer
	if err := dockerPool.Purge(mysqlContainer); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func CreateDatabaseTable(db *sql.DB) {
	var err error
	_, err = db.Exec("DROP TABLE abobd")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS abobd  (o int AUTO_INCREMENT PRIMARY KEY, aa nvarchar(1025), bb nvarchar(1025), cc nvarchar(1025), dd nvarchar(1025) )")
	fmt.Println("2")
	if err != nil {
		log.Fatal(err)
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890йцукенгшщзхъфывапролджэёячсмитьбюЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЁЯЧСМИТЬБЮ"

func RandStringBytes() string {
	b := make([]byte, 1024)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%194]
	}
	return string(b)
}

func FillDataBaseTable(db *sql.DB, count int) {
	var tx *sql.Tx
	var err error
	for i := 0; i < count; i++ {
		currentctx, currentcancel := context.WithTimeout(context.Background(), 12*time.Hour)
		defer currentcancel()
		tx, err = db.BeginTx(currentctx, nil)
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.ExecContext(currentctx, "INSERT INTO abobd(aa, bb, cc, dd) VALUES (?,?,?,?)", RandStringBytes(), RandStringBytes(), RandStringBytes(), RandStringBytes())
		if err != nil {
			log.Fatal(err, tx.Rollback())
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
		if i%100 == 0 {
			fmt.Println(strconv.Itoa(i))
		}
	}
}

func calculationPart(dbStd *sql.DB) plotter.XYs {
	var err error
	var xys plotter.XYs
	var durations = make(chan int64, 120)
	var averageTime int64
	var count = 0
	var done = make(chan struct{})
	var stats = make(chan []dockerstats.Stats, 180)
	hardTicker := time.NewTicker(5 * time.Second)
	mediumTicker := time.NewTicker(2 * time.Second)
	statTicker := time.NewTicker(1 * time.Second)

	go func(chan int64, chan struct{}, chan []dockerstats.Stats) {
		for {
			select {
			case <-statTicker.C:
				s, err := dockerstats.Current()
				if err != nil {
					log.Println("Unable to get stats ", err)
				}
				stats <- s
			case <-done:
				close(durations)
				close(stats)
				return
			case <-hardTicker.C:
				go func() {
					queryctx, querycancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer querycancel()
					fakeRows, err = dbStd.QueryContext(queryctx, HardQuery)
					if err != nil && err != context.DeadlineExceeded {
						log.Fatal("got error in hardquery:", err)
					}
					fmt.Println("hard query done")
				}()
			case <-mediumTicker.C:
				go func(chan int64) {
					start := time.Now()
					fakeRows, err = dbStd.Query(MediumQuery)
					if err != nil {
						log.Fatal("got error in MediumQuery ", err)
					}

					select {
					case _, is_open := <-done:
						if is_open {
							done <- struct{}{}
						} else {
						}
					default:
						d := time.Since(start).Milliseconds()
						log.Println("MediumQuery duration: ", d)
						durations <- d
					}
				}(durations)
			}
		}
	}(durations, done, stats)
	time.Sleep(180 * time.Second)
	hardTicker.Stop()
	mediumTicker.Stop()
	statTicker.Stop()
	done <- struct{}{}

	file, err := os.Create(MikeFilePath + driverName + ".csv")
	if err != nil {
		fmt.Println("Unable to create file:", err)
		os.Exit(1)
	}
	statfile, err := os.Create(MikeFilePath + driverName + "stats.csv")
	if err != nil {
		log.Fatal("Unable to create statfile", err)
		os.Exit(1)
	}
	defer file.Close()
	defer statfile.Close()

	for currentDuration := range durations {
		_, err = file.WriteString(fmt.Sprint(currentDuration) + "\n")
		if err != nil {
			log.Fatal("Unable to write in file", err)
		}
		buff := currentDuration
		averageTime += buff
		count++
		xys = append(xys, struct{ X, Y float64 }{float64(count), float64(buff)})
	}
	for stat := range stats {
		for _, s := range stat {
			_, err = statfile.WriteString(s.CPU + "," + s.Memory.Percent + "\n")
			if err != nil {
				log.Fatal("Cannot write into file", err)
			}
		}
	}
	averageTime = averageTime / int64(math.Max(float64(count), 1))
	fmt.Println("Average MediumQuery duration: ", averageTime)
	return xys
}
func connectToDB() *sql.DB {
	var err error
	var dbStd *sql.DB
	testMu.Lock()
	benchTestConfig := sqlConfig
	testMu.Unlock()
	benchTestConfig.DBName = "BigBench"
	if err != nil {
		log.Fatal(err)
	}
	dbStd, err = sql.Open(driverName, benchTestConfig.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}
	return dbStd
}

func TestDemo(t *testing.T) {
	var xys plotter.XYs
	dbStd := connectToDB()
	xys = calculationPart(dbStd)
	_ = xys
	makePlot(xys)
}
func ShowDatabases() {
	var err error
	var rows *sql.Rows
	rows, err = systemdb.Query("show databases")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	second_params := make([]string, 0)
	for rows.Next() {
		var second string
		if err := rows.Scan(&second); err != nil {
			log.Fatal(err)
		}
		second_params = append(second_params, second)
	}
	log.Println("all the bases")
	log.Println(strings.Join(second_params, " "))
}
func CheckRows(db *sql.DB) int {
	var err error
	var rows *sql.Rows
	queryctx, querycancel := context.WithTimeout(context.Background(), 100000000*time.Millisecond)
	defer querycancel()
	rows, err = db.QueryContext(queryctx, "select count(*) from abobd")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var first int
		if err := rows.Scan(&first); err != nil {
			log.Fatal(err)
		}
		return first
	}
	return 0
}
