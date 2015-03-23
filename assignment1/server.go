package main

import (
    "net"
    "strings"
    "strconv"
    "time"
    "sync"
)

type ds struct {
	val string
	timestamp int
	start_time time.Time
	numbyte int
	version int
}

var (
	m = make(map[string]ds)
	lock = &sync.RWMutex{}
)

func set(conn net.Conn, key string, timestamp int, numbytes int, reply string, val string) {	
	lock.RLock()
	ele, ok := m[key]
	lock.RUnlock()
	version := 0
	if ok == false {
		version = 1
	} else {
		version = ele.version + 1
	}
	lock.Lock()
	m[key] = ds{val, timestamp, time.Now(), numbytes, version}
	lock.Unlock()
	conn.Write([]byte("OK"))
}

func get(conn net.Conn, key string) {
	lock.RLock()
	val11 := m[key]
	lock.RUnlock()
	conn.Write([]byte(val11.val))
}

func handleConnection(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		size, err := conn.Read(buf)
		if err != nil {
			//fmt.Println("Error reading:", err.Error())
		}
		str := string(buf)
		str = str[:size]
		fields := strings.Split(str," ")
		if len(fields) == 1 {
			break
		}
		command := strings.TrimSpace(fields[0])
		key := strings.TrimSpace(fields[1])
		if command == "set" {
			timestamp,_ := strconv.Atoi(strings.TrimSpace(fields[2]))
			numbytes,_ := strconv.Atoi(strings.TrimSpace(fields[3]))
			reply := "no"
			if strings.TrimSpace(fields[4]) == "[noreply]" {
				reply = "no"
			} else {
				reply = "yes"
			}
			val := strings.TrimSpace(fields[5])
			set(conn, key, timestamp, numbytes, reply, val)
		} else if command == "get" {
			get(conn, key)
		} else {
			
		}
	}
}

func main() {
    ln, err := net.Listen("tcp", ":9000")
    if err != nil {
        // handle error
    }
    for {
        conn, err := ln.Accept()
        if err != nil {
            // handle error
            continue
        }
        go handleConnection(conn)
    }
}
