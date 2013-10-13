package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const VERSION = "0.1"

type Destination struct {
	Address string
	Regex   *regexp.Regexp
	Replace []byte
}

type Packet struct {
	Key  []byte
	Body []byte
}

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return fmt.Sprint(*a)
}

var (
	address              = flag.String("address", ":8125", "UDP listening address")
	destinationAddresses = StringArray{}
	showVersion          = flag.Bool("version", false, "print version info")
)

func init() {
	flag.Var(&destinationAddresses, "destination-address", "destination address (may be given multiple times)")
}

var packetRegexp = regexp.MustCompile("^([^:]+):(.*)$")

func parseMessage(data []byte) []*Packet {
	var output []*Packet
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		item := packetRegexp.FindSubmatch(line)
		if len(item) == 0 {
			continue
		}

		packet := &Packet{
			Key:  item[1],
			Body: item[2],
		}
		output = append(output, packet)
	}
	return output
}

func udpListener(destinations []Destination) {
	addr, _ := net.ResolveUDPAddr("udp", *address)
	log.Printf("listening on %s", addr)
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()

	var destConns []net.Conn
	for _, destination := range destinations {
		conn, err := net.DialTimeout("udp", destination.Address, time.Second)
		if err != nil {
			log.Fatalf("ERROR: UDP connection failed - %s", err)
		}
		destConns = append(destConns, conn)
	}

	message := make([]byte, 512)
	for {
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		log.Printf("message: %s (%d)", message[:n], n)
		for _, p := range parseMessage(message[:n]) {
			for i, destination := range destinations {
				key := destination.Regex.ReplaceAll(p.Key, destination.Replace)
				packet := fmt.Sprintf("%s:%s", key, p.Body)
				conn := destConns[i]
				_, err := conn.Write([]byte(packet))
				if err != nil {
					log.Printf("ERROR: writing to UDP socket - %s", err)
					conn.Close()

					// reconnect
					conn, err := net.DialTimeout("udp", destination.Address, time.Second)
					if err != nil {
						log.Fatalf("ERROR: UDP connection failed - %s", err)
					}
					destConns[i] = conn
				}
			}
		}
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdtee v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	var destinations []Destination
	for _, destinationAddress := range destinationAddresses {
		parts := strings.Split(destinationAddress, ":")
		destinations = append(destinations, Destination{
			Address: fmt.Sprintf("%s:%s", parts[0], parts[1]),
			Regex:   regexp.MustCompile(parts[2]),
			Replace: []byte(parts[3]),
		})
	}

	if len(destinations) == 0 {
		log.Fatalf("must specify at least one --destination-address")
	}

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)

	udpListener(destinations)
}
