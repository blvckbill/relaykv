package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	resp "github.com/blvckbill/redis-from-scratch/internal/protocol"
	"github.com/blvckbill/redis-from-scratch/internal/store"
)

type Server struct {
	store       *store.Store
	aof         *AOFLogger
	channels    map[string]map[net.Conn]bool
	pubsubMu    sync.RWMutex
	isReplaying atomic.Bool
	replicas    []net.Conn
	replicaMu   sync.Mutex
	isReplica   bool
}

func NewServer() *Server {
	var db = store.NewStore()
	aofLogger, err := NewAOFLogger("appendonly.aof")
	channels := make(map[string]map[net.Conn]bool)
	replicas := make([]net.Conn, 0)
	if err != nil {
		log.Fatalf("Fatal: could not create AOF logger: %v", err)
	}

	s := &Server{
		store:    db,
		aof:      aofLogger,
		channels: channels,
		replicas: replicas,
	}
	s.isReplaying.Store(true)
	aofLogger.Replay(s)
	s.isReplaying.Store(false)

	return s
}

func (s *Server) Start() {
	addr := "127.0.0.1:6369"
	// Create a TCP listening socket bound to the address.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// if there is an error, fail fast like redis does
		log.Fatalf("Fatal: could not start listener: %v", err)
	}

	defer ln.Close()

	log.Printf("GoRedis is listening on %s", addr)

	// create a loop to wait for an Accept on the listener
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Hnadle Accept error
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				fmt.Printf("Timeout occurred, just waiting for the next caller...")
				continue
			}
			log.Fatalf("Accept error: %v", err)
		}
		// once there is a connection, hand it off to another process using go concurrency so bloacking is avoided
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Connection established successfully")
	// create a buffer to read data from the connection and write it back to the client
	readBuf := make([]byte, 4096)
	var buffer []byte
	// create a loop to read from the connection into the buffer and write back to the client
	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Printf("Connection disconnected or error: %v", err)
				break
			}
		}
		// append the read data to the buffer and try to parse it as RESP
		buffer = append(buffer, readBuf[:n]...)
		for {
			// Try to parse the buffer as RESP, if successful, execute the command and write the response back to the client
			parsedResp, consumed, ok := resp.Parser(buffer)
			if !ok {
				break
			}
			// Remove the parsed command from the buffer
			buffer = buffer[consumed:]

			fmt.Printf("Parsed RESP: %+v\n", parsedResp)
			// Execute the command and write the response back to the client
			parsed, ok := ParsedRespToStrings(parsedResp)
			if !ok {
				log.Printf("Error parsing RESP to strings")
				return
			}
			response := s.commandExecution(conn, parsed)

			if conn != nil && response != nil {
				bytes_parsed := respEncoder(response)

				_, err := conn.Write(bytes_parsed)
				if err != nil {
					log.Printf("Error writing to connection: %v", err)
					return
				}

				fmt.Printf("Sent response: %s", string(bytes_parsed))
			}
		}
	}
}

/*
commandExecution takes a slice of strings representing the command and its arguments,
executes the command, and returns a RESP response.
*/
func (s *Server) commandExecution(conn net.Conn, argv []string) *resp.Resp {
	if len(argv) == 0 {
		return nil
	}

	cmd := strings.ToUpper(argv[0])

	writeCmds := map[string]bool{"SET": true, "DEL": true, "INCR": true, "LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true}
	if s.isReplica && writeCmds[cmd] {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("READONLY You can't write against a read only replica"),
		}
	}

	var response *resp.Resp
	switch cmd { // refactor to use interfaces
	case "PING":
		return s.handlePing(argv[1:])
	case "ECHO":
		return s.handleEcho(argv[1:])
	case "SET":
		response = s.handleSet(argv[1:])
	case "GET":
		return s.handleGet(argv[1:])
	case "DEL":
		response = s.handleDel(argv[1:])
	case "INCR":
		response = s.handleIncr(argv[1:])
	case "TTL":
		return s.handleTTL(argv[1:])
	case "LPUSH":
		response = s.handleLPush(argv[1:])
	case "RPUSH":
		response = s.handleRPush(argv[1:])
	case "LPOP":
		response = s.handleLPop(argv[1:])
	case "RPOP":
		response = s.handleRPop(argv[1:])
	case "LRANGE":
		return s.handleLRange(argv[1:])
	case "SUBSCRIBE":
		return s.handleSubscribe(conn, argv[1:])
	case "PUBLISH":
		response = s.handlePublish(argv[1:])
	case "UNSUBSCRIBE":
		return s.handleUnsubscribe(conn, argv[1:])
	case "REPLICAOF":
		return s.handleReplicaOf(conn)
	default:
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR unknown command"),
		}
	}

	cmdBytes := encodeCommand(argv)

	if !s.isReplaying.Load() {
		if err := s.aof.Append(cmdBytes); err != nil {
			log.Printf("AOF append error: %v", err)
		}
		s.propagateToReplicas(cmdBytes)
	}

	return response
}

func encodeCommand(argv []string) []byte {
	r := &resp.Resp{
		Type:  resp.Array,
		Array: make([]*resp.Resp, len(argv)),
	}
	for i, arg := range argv {
		s := arg
		r.Array[i] = &resp.Resp{
			Type: resp.BulkString,
			Str:  &s,
		}
	}
	return respEncoder(r)
}

// strPtr is a helper function to create a pointer to a string literal.
func strPtr(s string) *string {
	return &s
}

/*
respToString takes a RESP object and converts it to a string if possible.
It returns the string and a boolean indicating whether the conversion was successful.
Only RESP types that can be represented as strings (BulkString, SimpleString, Integer) are converted.
Other types will return an empty string and false.
*/
func respToString(r *resp.Resp) (string, bool) {
	switch r.Type {
	case resp.BulkString, resp.SimpleString:
		if r.Str == nil {
			return "", false
		}
		return *r.Str, true

	case resp.Integer:
		return fmt.Sprintf("%d", r.Int), true

	default:
		return "", false
	}
}

/*
ParsedRespToStrings takes a parsed RESP command and converts
it to a slice of strings representing the command and its arguments.
*/
func ParsedRespToStrings(r *resp.Resp) ([]string, bool) {
	var argv []string
	for _, arg := range r.Array {
		s, ok := respToString(arg)
		if !ok {
			log.Printf("Error converting RESP to string: %v", arg)
			return nil, false
		}
		argv = append(argv, s)
	}
	return argv, true
}

/*
respEncoder takes a RESP object and encodes it into a byte slice that can be sent back to the client.
It handles all RESP types (SimpleString, Error, Integer, BulkString, Array) and returns the appropriate RESP format as bytes.
If an unknown RESP type is encountered, it returns an error message in RESP format.
*/
func respEncoder(r *resp.Resp) []byte {
	switch r.Type {

	case resp.SimpleString:
		return []byte("+" + *r.Str + "\r\n")

	case resp.Error:
		return []byte("-" + *r.Str + "\r\n")

	case resp.Integer:
		return []byte(":" + strconv.FormatInt(r.Int, 10) + "\r\n")

	case resp.BulkString:
		if r.Str == nil {
			return []byte("$-1\r\n")
		}
		s := *r.Str
		return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")

	case resp.Array:
		if r.Array == nil {
			return []byte("*-1\r\n")
		}
		var out []byte

		out = append(out, []byte("*"+strconv.Itoa(len(r.Array))+"\r\n")...)

		for _, el := range r.Array {
			out = append(out, respEncoder(el)...)
		}
		return out
	}
	return []byte("-ERR unknown RESP type\r\n")
}
