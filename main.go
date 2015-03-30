package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"time"
)

type Log struct {
	Meta *Meta `json:"meta"`
	Data *Data `json:"data"`
}

type Meta struct {
	Process string `json:"process_ctx_id"`
	Ctx     string `json:"ctx_id"`
}

type Data struct {
	ParentCtx   string `json:"parent_ctx_id"` // TODO: this should not be here
	Service     string `json:"service"`
	Environment string `json:"environment,omitempty"`
	HostName    string `json:"hostname"`
	Timestamp   string `json:"timestamp"`
	Severity    string `json:"severity"`
	Application string `json:"application"`
	CallerLine  string `json:"caller_line,omitempty"`
	CallerFile  string `json:"caller_file,omitempty"`
	Message     string `json:"msg"`
}

var (
	help     = flag.Bool("help", false, "print help")
	topic    = flag.String("topic", "log.raw#ephemeral", "NSQ Topic")
	endpoint = flag.String("endpoint", "", "NSQ Endpoint 'host:port'")
	app      = flag.String("app", "", "Application name")
	svc      = flag.String("svc", "", "Service name")
	host     = host_gen()
	uuid     = uuid_gen()
)

func initialize() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	_help()
	// friendly validate some user input
	_validate_endpoint()
	_validate_topic()
	//TODO croak unless app and svc names
}

func main() {

	initialize()

	fmt.Println("Producing logs to '", *topic, "' on '", *endpoint, "'")

	// initialize STDIN buffed io scanner
	scanner := bufio.NewScanner(os.Stdin)

	// initialize NSQ producer
	config := nsq.NewConfig()
	w, _ := nsq.NewProducer(*endpoint, config)

	// scan STDIN
	for scanner.Scan() {
		// ignore empty lines
		if regexp.MustCompile(`^\s*$`).MatchString(scanner.Text()) != true {
			send_log_to_nsq(w, scanner.Text())
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

func _build_message(msg string) *Log {
	var log *Log = &Log{
		Meta: &Meta{
			Process: uuid,
			Ctx:     uuid,
		},
		Data: &Data{
			ParentCtx:   uuid,
			Service:     *svc,
			HostName:    host,
			Timestamp:   get_date(),
			Severity:    "raw",
			Application: *app,
			Message:     msg,
		},
	}

	return log
}

func send_log_to_nsq(nsq *nsq.Producer, line string) {
	byte_msg := []byte(line)
	var log *Log
	json.Unmarshal(byte_msg, &log)
	msg := byte_msg
	if log == nil {
		var err error
		msg, err = json.Marshal(_build_message(line))
		if err != nil {
			fmt.Println("Problem marshal'ing string ", err)
		}
	}

	if len(msg) > 0 {
		fmt.Println(string(msg))
		nsq.Publish(*topic, msg)
	}

}

func uuid_gen() string {
	uuid, err := exec.Command("/usr/bin/uuidgen").Output()
	if err != nil {
		fmt.Println("Problem getting hostname")
	}
	return chop(string(uuid))

}

func chop(s string) string {
	return s[0 : len(s)-1]
}

func host_gen() string {
	host, err := os.Hostname()
	if err != nil {
		fmt.Println("Problem getting hostname")
	}
	return host
}

func _help() {
	if *help {
		fmt.Println(`
Usage:
    orc-golog --help     obtain help
    orc-golog --endpoint NSQ endpoint
    orc-golog --app      name of the application
    orc-golog --svc      name of the service
    orc-golog --topic    topic for logging (defaults to 'log.raw#ephemeral')

    orc-golog --app <your_app_name> --svc <your_service_name> --endpoint 127.0.0.1:4150
`)
		os.Exit(0)
	}
}

func _validate_endpoint() {
	if regexp.MustCompile(`\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}`).MatchString(*endpoint) != true {
		fmt.Print("endpoint: '", *endpoint, "' is invalid\n")
		os.Exit(0)
	}
}

func _validate_topic() {
	// topics must always be ephemeral
	s := regexp.MustCompile("#").Split(*topic, 2)
	if len(s) > 1 && s[1] != "ephemeral" {
		fmt.Print(*topic, " has been renamed to ", s[0], "#ephemeral", "\n")
		*topic = s[0] + "#ephemeral"
	}

	if regexp.MustCompile("#ephemeral$").MatchString(*topic) != true {
		fmt.Print(*topic, " has been renamed to ", *topic, "#ephemeral", "\n")
		*topic = *topic + "#ephemeral"
	}
}

func get_date() string {
	return time.Now().Format("2006-01-02T15:04:05.999999Z")
}
