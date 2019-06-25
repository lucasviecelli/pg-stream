package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
)

func main() {
	var err error

	// if replicationConnConfig == nil {
	// 	fmt.Println("Skipping due to undefined replicationConnConfig")
	// }

	// conn := mustConnect(t, *replicationConnConfig)

	conn, err := pgx.Connect(pgx.ConnConfig{Database: "stream", User: "postgres"})
	if err != nil {
		log.Fatal("Error connecting: ", err)
	}

	defer func() {
		// Ensure replication slot is destroyed, but don't check for errors as it
		// should have already been destroyed.
		conn.Exec("select pg_drop_replication_slot('pgx_test')")
		// close
	}()

	conn.Exec("select pg_drop_replication_slot('pgx_test')")

	replicationConn, errRepli := pgx.ReplicationConnect(pgx.ConnConfig{Database: "stream", User: "postgres"})
	if errRepli != nil {
		log.Fatal("Error connecting replication: ", errRepli)
	}

	var cp string
	var snapshot_name string
	cp, snapshot_name, err = replicationConn.CreateReplicationSlotEx("pgx_test", "test_decoding")
	if err != nil {
		log.Fatal("replication slot create failed: %v", err)
	}
	if cp == "" {
		log.Print("consistent_point is empty")
	}
	if snapshot_name == "" {
		log.Print("snapshot_name is empty")
	}

	// Do a simple change so we can get some wal data
	_, err = conn.Exec("create table if not exists replication_test (a integer)")
	if err != nil {
		log.Fatal("Failed to create table: %v", err)
	}

	err = replicationConn.StartReplication("pgx_test", 0, -1)
	if err != nil {
		log.Fatal("Failed to start replication: %v", err)
	}

	var insertedTimes []int64
	currentTime := time.Now().Unix()

	for i := 0; i < 5; i++ {
		var ct pgx.CommandTag
		insertedTimes = append(insertedTimes, currentTime)
		ct, err = conn.Exec("insert into replication_test(a) values($1)", currentTime)
		if err != nil {
			log.Fatal("Insert failed: %v", err)
		}
		log.Print("Inserted %d rows", ct.RowsAffected())
		currentTime++
	}

	var foundTimes []int64
	var foundCount int
	var maxWal uint64

	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()

	for {
		var message *pgx.ReplicationMessage

		message, err = replicationConn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatal("Replication failed: %v %s", err, reflect.TypeOf(err))
		}

		if message.WalMessage != nil {
			// The waldata payload with the test_decoding plugin looks like:
			// public.replication_test: INSERT: a[integer]:2
			// What we wanna do here is check that once we find one of our inserted times,
			// that they occur in the wal stream in the order we executed them.
			walString := string(message.WalMessage.WalData)

			if strings.Contains(walString, "public.replication_test: INSERT") {
				stringParts := strings.Split(walString, ":")
				offset, err := strconv.ParseInt(stringParts[len(stringParts)-1], 10, 64)
				if err != nil {
					log.Fatal("Failed to parse walString %s", walString)
				}
				if foundCount > 0 || offset == insertedTimes[0] {
					foundTimes = append(foundTimes, offset)

					fmt.Print(foundTimes)

					foundCount++
				}
				if foundCount == len(insertedTimes) {
					break
				}
			}
			if message.WalMessage.WalStart > maxWal {
				maxWal = message.WalMessage.WalStart
			}

		}
		if message.ServerHeartbeat != nil {
			log.Print("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}

	for i := range insertedTimes {
		if foundTimes[i] != insertedTimes[i] {
			log.Fatal("Found %d expected %d", foundTimes[i], insertedTimes[i])
		}
	}

	log.Print("Found %d times, as expected", len(foundTimes))

	// Before closing our connection, let's send a standby status to update our wal
	// position, which should then be reflected if we fetch out our current wal position
	// for the slot
	status, err := pgx.NewStandbyStatus(maxWal)
	if err != nil {
		log.Print("Failed to create standby status %v", err)
	}
	replicationConn.SendStandbyStatus(status)

	restartLsn := getConfirmedFlushLsnFor(conn, "pgx_test")
	integerRestartLsn, _ := pgx.ParseLSN(restartLsn)
	if integerRestartLsn != maxWal {
		log.Fatal("Wal offset update failed, expected %s found %s", pgx.FormatLSN(maxWal), restartLsn)
	}

	// pgx.closeReplicationConn(t, replicationConn)

	// replicationConn2 := mustReplicationConnect(t, *replicationConnConfig)
	// defer closeReplicationConn(t, replicationConn2)

	// err = replicationConn2.DropReplicationSlot("pgx_test")
	// if err != nil {
	// 	t.Fatalf("Failed to drop replication slot: %v", err)
	// }

	// droppedLsn := getConfirmedFlushLsnFor(conn, "pgx_test")
	// if droppedLsn != "" {
	// 	t.Errorf("Got odd flush lsn %s for supposedly dropped slot", droppedLsn)
	// }

}

func getConfirmedFlushLsnFor(conn *pgx.Conn, slot string) string {
	// Fetch the restart LSN of the slot, to establish a starting point
	rows, err := conn.Query(fmt.Sprintf("select confirmed_flush_lsn from pg_replication_slots where slot_name='%s'", slot))
	if err != nil {
		log.Fatal("conn.Query failed: %v", err)
	}
	defer rows.Close()

	var restartLsn string
	for rows.Next() {
		rows.Scan(&restartLsn)
	}
	return restartLsn
}
