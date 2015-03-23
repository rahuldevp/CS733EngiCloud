package main

import (
    "fmt"
    "log"
	"time"
	"testing"
    "net"
	"strings"
)

var ret string

type cases struct {
input, desired string
}

func TestCase(t *testing.T) {
	go main()	
	for i:=1;i<=100;i++ {
		go multiclient()
	}
	time.Sleep(time.Second)
}

func multiclient() {
	test_cases := []cases {
				{"set Rahul 200 10 [noreply] dev","OK"},
				{"set Prakash 300 20 [noreply] agrawal","OK"},
				{"set Rahul 2000 100 [noreply] dev1","OK"},
				{"get Rahul","dev1"},
	}
	flag := 0
	for _, cases := range test_cases {
		conn, err := net.Dial("tcp", "localhost:9000")
	    if err != nil {
	        log.Fatal("Connection error", err)
	    }
	    buf := make([]byte, 1024)
	    conn.Write([]byte(cases.input))
	    size, err := conn.Read(buf)
		respon := string(buf)[:size]
		respon = strings.TrimSpace(respon)
		if respon != cases.desired {
			fmt.Println("FAIL")
			flag = 1
			break
		}
    	defer conn.Close()
	}
	if flag == 0 {
	   	fmt.Println("PASS")
	}
}
