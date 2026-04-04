package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	resp "github.com/blvckbill/redis-from-scratch/internal/protocol"
)

/*
handlePing takes the arguments for the PING command and returns a RESP response.
If no arguments are provided, it returns "PONG".
If one argument is provided, it returns that argument as a bulk string.
If more than one argument is provided, it returns an error.
*/
func (srv *Server) handlePing(args []string) *resp.Resp {
	if len(args) == 0 {
		return &resp.Resp{
			Type: resp.SimpleString,
			Str:  strPtr("PONG"),
		}
	}

	if len(args) > 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'ping' command"),
		}
	}

	s := args[0]
	return &resp.Resp{
		Type: resp.BulkString,
		Str:  &s,
	}
}

/*
handleEcho takes the arguments for the ECHO command and returns a RESP response.
If no arguments are provided, it returns an error.
If one or more arguments are provided, it concatenates them with spaces and returns the result as a bulk string.
*/
func (srv *Server) handleEcho(args []string) *resp.Resp {
	if len(args) < 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR nothing to ECHO"),
		}
	}

	s := strings.Join(args, " ")
	return &resp.Resp{
		Type: resp.BulkString,
		Str:  &s,
	}
}

func (srv *Server) handleSet(args []string) *resp.Resp {
	if len(args) < 2 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'set' command"),
		}
	}

	key := args[0]
	val := args[1]
	var ttl int64
	// SET key value EX seconds
	if len(args) == 4 && strings.ToUpper(args[2]) == "EX" {
		ttl, _ = strconv.ParseInt(args[3], 10, 64)
	}

	srv.store.Set(key, val, ttl)

	return &resp.Resp{
		Type: resp.SimpleString,
		Str:  strPtr("OK"),
	}
}

func (srv *Server) handleGet(args []string) *resp.Resp {
	if len(args) != 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'get' command"),
		}
	}

	key := args[0]
	val, ok := srv.store.Get(key)
	if !ok {
		return &resp.Resp{
			Type: resp.BulkString,
			Str:  nil,
		}
	}

	return &resp.Resp{
		Type: resp.BulkString,
		Str:  &val,
	}
}

func (srv *Server) handleIncr(args []string) *resp.Resp {
	if len(args) != 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'incr' command"),
		}
	}

	key := args[0]
	val, err := srv.store.Incr(key)
	if err != nil {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR value is not an integer or out of range"),
		}
	}

	return &resp.Resp{
		Type: resp.Integer,
		Int:  val,
	}
}

func (s *Server) handleDel(args []string) *resp.Resp {
	if len(args) < 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'del' command"),
		}
	}

	cnt := s.store.Del(args)

	return &resp.Resp{
		Type: resp.Integer,
		Int:  int64(cnt),
	}
}

func (s *Server) handleTTL(args []string) *resp.Resp {
	if len(args) != 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'ttl' command"),
		}
	}

	key := args[0]
	ttl := s.store.TTL(key)

	return &resp.Resp{
		Type: resp.Integer,
		Int:  ttl,
	}
}

func (s *Server) handleLPush(args []string) *resp.Resp {
	if len(args) < 2 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'LPUSH'"),
		}
	}

	key := args[0]
	values := args[1:]

	length, err := s.store.LPush(key, values...)
	if err != nil {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr(err.Error()),
		}
	}

	return &resp.Resp{
		Type: resp.Integer,
		Int:  int64(length),
	}
}

func (s *Server) handleRPush(args []string) *resp.Resp {
	if len(args) < 2 {
		return &resp.Resp{Type: resp.Error, Str: strPtr("ERR wrong number of arguments")}
	}

	key := args[0]
	values := args[1:]

	length, err := s.store.RPush(key, values...)
	if err != nil {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr(err.Error()),
		}
	}

	return &resp.Resp{
		Type: resp.Integer,
		Int:  int64(length),
	}
}

func (s *Server) handleLPop(args []string) *resp.Resp {
	if len(args) != 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'LPOP'"),
		}
	}

	key := args[0]

	val, ok := s.store.LPop(key)

	if !ok {
		return &resp.Resp{
			Type: resp.BulkString,
			Str:  nil,
		}
	}

	return &resp.Resp{
		Type: resp.BulkString,
		Str:  &val,
	}
}

func (s *Server) handleRPop(args []string) *resp.Resp {
	if len(args) != 1 {
		return &resp.Resp{
			Type: resp.BulkString,
			Str:  nil,
		}
	}

	key := args[0]

	val, ok := s.store.RPop(key)

	if !ok {
		return &resp.Resp{
			Type: resp.BulkString,
			Str:  nil,
		}
	}

	return &resp.Resp{
		Type: resp.BulkString,
		Str:  &val,
	}
}

func (s *Server) handleLRange(args []string) *resp.Resp {
	if len(args) != 3 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'LRANGE'"),
		}
	}

	key := args[0]

	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])

	if err1 != nil || err2 != nil {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR value is not an integer or out of range"),
		}
	}

	values := s.store.LRange(key, start, stop)

	respArr := make([]*resp.Resp, len(values))

	for i, v := range values {
		val := v
		respArr[i] = &resp.Resp{
			Type: resp.BulkString,
			Str:  &val,
		}
	}

	return &resp.Resp{
		Type:  resp.Array,
		Array: respArr,
	}
}

func (s *Server) handleSubscribe(conn net.Conn, args []string) *resp.Resp {
	if conn == nil {
		return nil
	}
	if len(args) < 1 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'SUBSCRIBE'"),
		}
	}

	for _, ch := range args {
		// add connection to channel
		s.pubsubMu.Lock()
		subs, ok := s.channels[ch]
		if !ok {
			subs = make(map[net.Conn]bool)
			s.channels[ch] = subs
		}
		subs[conn] = true

		// count how many channels this connection is subscribed to
		count := 0
		for _, subscribers := range s.channels {
			if subscribers[conn] {
				count++
			}
		}
		s.pubsubMu.Unlock()

		// send one response per channel
		chName := ch
		respArr := []*resp.Resp{
			{Type: resp.BulkString, Str: strPtr("subscribe")},
			{Type: resp.BulkString, Str: &chName},
			{Type: resp.Integer, Int: int64(count)},
		}
		conn.Write(respEncoder(&resp.Resp{
			Type:  resp.Array,
			Array: respArr,
		}))
	}

	return nil
}

func (s *Server) handlePublish(args []string) *resp.Resp {
	if len(args) != 2 {
		return &resp.Resp{
			Type: resp.Error,
			Str:  strPtr("ERR wrong number of arguments for 'PUBLISH'"),
		}
	}

	channel := args[0]
	message := args[1]

	// build the message to push to each subscriber
	notification := &resp.Resp{
		Type: resp.Array,
		Array: []*resp.Resp{
			{Type: resp.BulkString, Str: strPtr("message")},
			{Type: resp.BulkString, Str: &channel},
			{Type: resp.BulkString, Str: &message},
		},
	}
	encoded := respEncoder(notification)

	s.pubsubMu.RLock()
	subs, ok := s.channels[channel]
	if !ok {
		s.pubsubMu.RUnlock()
		// no subscribers
		return &resp.Resp{
			Type: resp.Integer,
			Int:  0,
		}
	}

	// copy subscriber connections before releasing lock
	// so we don't hold the lock while writing to each conn
	receivers := make([]net.Conn, 0, len(subs))
	for conn := range subs {
		receivers = append(receivers, conn)
	}
	s.pubsubMu.RUnlock()

	// write to each subscriber outside the lock
	delivered := 0
	for _, conn := range receivers {
		_, err := conn.Write(encoded)
		if err != nil {
			// subscriber disconnected — remove them
			s.pubsubMu.Lock()
			delete(s.channels[channel], conn)
			s.pubsubMu.Unlock()
		} else {
			delivered++
		}
	}

	// return number of subscribers the message was delivered to
	return &resp.Resp{
		Type: resp.Integer,
		Int:  int64(delivered),
	}
}

func (s *Server) handleUnsubscribe(conn net.Conn, args []string) *resp.Resp {
	if conn == nil {
		return nil
	}
	// if no args, unsubscribe from all channels this conn is in
	if len(args) == 0 {
		s.pubsubMu.Lock()
		for ch, subs := range s.channels {
			if subs[conn] {
				args = append(args, ch)
			}
		}
		s.pubsubMu.Unlock()
	}

	for _, ch := range args {
		s.pubsubMu.Lock()

		// remove connection from this channel
		if subs, ok := s.channels[ch]; ok {
			delete(subs, conn)
			// if channel is now empty, remove it entirely
			if len(subs) == 0 {
				delete(s.channels, ch)
			}
		}

		// count remaining subscriptions for this connection
		count := 0
		for _, subs := range s.channels {
			if subs[conn] {
				count++
			}
		}
		s.pubsubMu.Unlock()

		chName := ch
		respArr := []*resp.Resp{
			{Type: resp.BulkString, Str: strPtr("unsubscribe")},
			{Type: resp.BulkString, Str: &chName},
			{Type: resp.Integer, Int: int64(count)},
		}
		conn.Write(respEncoder(&resp.Resp{
			Type:  resp.Array,
			Array: respArr,
		}))
	}

	return nil
}

func (s *Server) handleReplicaOf(conn net.Conn) *resp.Resp {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()

	s.replicas = append(s.replicas, conn)
	return &resp.Resp{
		Type: resp.SimpleString,
		Str:  strPtr("OK"),
	}
}

func (s *Server) propagateToReplicas(cmd []byte) {
	s.replicaMu.Lock()
	defer s.replicaMu.Unlock()

	log.Printf("Propagating to %d replicas: %s", len(s.replicas), string(cmd))

	alive := s.replicas[:0]
	for _, conn := range s.replicas {
		_, err := conn.Write(cmd)
		if err != nil {
			log.Printf("Replica disconnected: %v", err)
			conn.Close()
		} else {
			alive = append(alive, conn)
		}
	}
	s.replicas = alive
}

func (s *Server) StartReplication(primaryAddr string) error {
	// connect to the primary as a client
	conn, err := net.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("could not connect to primary at %s: %v", primaryAddr, err)
	}

	// tell the primary we want to replicate
	cmd := encodeCommand([]string{"REPLICAOF"})
	_, err = conn.Write(cmd)
	if err != nil {
		return fmt.Errorf("could not send REPLICAOF to primary: %v", err)
	}
	s.isReplica = true
	// read the OK response back
	buf := make([]byte, 256)
	_, err = conn.Read(buf)
	if err != nil {
		return fmt.Errorf("no response from primary: %v", err)
	}

	log.Printf("Connected to primary at %s, starting replication", primaryAddr)

	// start a goroutine that reads commands from the primary
	// and feeds them through commandExecution
	go s.replicationLoop(conn)

	return nil
}

func (s *Server) replicationLoop(conn net.Conn) {
	defer conn.Close()

	readBuf := make([]byte, 4096)
	var buffer []byte

	for {
		n, err := conn.Read(readBuf)
		if err != nil {
			if err == io.EOF {
				log.Println("Primary closed the connection")
			} else {
				log.Printf("Replication error: %v", err)
			}
			return
		}

		buffer = append(buffer, readBuf[:n]...)

		for {
			parsed, consumed, ok := resp.Parser(buffer)
			if !ok {
				break
			}
			buffer = buffer[consumed:]

			argv, ok := ParsedRespToStrings(parsed)
			if !ok {
				continue
			}

			// apply the command locally — pass nil for conn since
			// replicated commands don't need to write back to anyone
			s.isReplaying.Store(true)
			s.commandExecution(nil, argv)
			s.isReplaying.Store(false)
		}
	}
}
