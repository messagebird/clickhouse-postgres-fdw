package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

type chserver struct {
	conn                *sql.DB
	clickhouseHosts     string
	clickhouseHostname  string
	clickhousePort      string
	clickhouseUsername  string
	clickhousePassword  string
	clickhouseDebugFlag string
	initialized         bool
}

func (ch *chserver) connect() error {
	if ch.initialized {
		return nil
	}

	// hosts := []string{}

	cHosts := strings.Split(ch.clickhouseHosts, ",")

	if ch.clickhouseUsername == "" {
		ch.clickhouseUsername = "default"
	}
	uri := fmt.Sprintf(
		"tcp://%s:%s?debug=%s&username=%s&password=%s",
		cHosts[0], ch.clickhousePort, ch.clickhouseDebugFlag, ch.clickhouseUsername, ch.clickhousePassword,
	)
	connect, err := sql.Open("clickhouse", uri)
	if err != nil {
		infoLogger.Fatalln("Encountered an error while creating ClickHouse client")
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = connect.PingContext(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			errLogger.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			errLogger.Printf("client ping")
		}
		return err
	}

	ch.conn = connect
	ch.initialized = true

	return nil
}

func (ch *chserver) query(query string) (*sql.Rows, error) {

	rows, err := ch.conn.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func parseServerOptions(opt Options) *chserver {

	ch := &chserver{}
	for key, value := range opt {
		if key == "host" {
			ch.clickhouseHosts = value
		} else if key == "port" {
			ch.clickhousePort = value
		}
	}
	ch.clickhouseDebugFlag = "true"

	return ch
}
