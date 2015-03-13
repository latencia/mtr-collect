package main

import (
	"./mqttc"
	"encoding/json"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	log "github.com/Sirupsen/logrus"
	"github.com/marpaia/graphite-golang"
	"gopkg.in/alecthomas/kingpin.v1"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	Graphite *graphite.Graphite
	NOOP     = false
	VERBOSE  = true
)

type Host struct {
	IP          string  `json:"ip"`
	Name        string  `json:"hostname"`
	Hop         int     `json:"hop-number"`
	Sent        int     `json:"sent"`
	LostPercent float64 `json:"lost-percent"`
	Last        float64 `json:"last"`
	Avg         float64 `json:"avg"`
	Best        float64 `json:"best"`
	Worst       float64 `json:"worst"`
	StDev       float64 `json:"standard-dev"`
}

type Report struct {
	Time        time.Time       `json:"time"`
	Hosts       []*Host         `json:"hosts"`
	Hops        int             `json:"hops"`
	ElapsedTime time.Duration   `json:"elapsed_time"`
	Location    *ReportLocation `json:"location"`
}

// slightly simpler struct than the one provided by geoipc
type ReportLocation struct {
	IP          string  `json:"ip"`
	CountryCode string  `json:"country_code"`
	CountryName string  `json:"country_name"`
	City        string  `json:"city"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
}

func sendMetrics(client *mqtt.MqttClient, msg mqtt.Message) {
	var report Report
	err := json.Unmarshal(msg.Payload(), &report)
	if err != nil {
		log.Error("Error decoding json report")
	}

	ip := report.Location.IP
	if ip == "" {
		log.Warnf("Discarding report with no source IP")
		return
	}
	ip = strings.Replace(ip, ".", "_", -1)

	cc := report.Location.CountryCode
	if cc == "" {
		log.Warnf("Discarding report with no country code")
		return
	}

	city := strings.Replace(report.Location.City, " ", "_", -1)
	hops := report.Hops
	if city == "" {
		city = "nil"
	}

	pktLossHops := 0
	for _, host := range report.Hosts {
		if host.LostPercent != 0 && host.LostPercent != 100 {
			pktLossHops += 1
		}
	}

	last_hop := report.Hosts[len(report.Hosts)-1]

	metric_prefix := strings.ToLower(fmt.Sprintf("push-mtr.%s.%s.%s", ip, cc, city))
	mHops := metric_prefix + ".hops"
	mLastHopAvg := metric_prefix + ".last_hop.avg"
	mLastHopBest := metric_prefix + ".last_hop.best"
	mLastHopWorst := metric_prefix + ".last_hop.worst"

	sendMetric(metric_prefix+".pkt_loss_hops", strconv.Itoa(pktLossHops))
	sendMetric(mHops, strconv.Itoa(hops))
	sendMetric(mLastHopAvg, strconv.FormatFloat(last_hop.Avg, 'f', 3, 64))
	sendMetric(mLastHopBest, strconv.FormatFloat(last_hop.Best, 'f', 3, 64))
	sendMetric(mLastHopWorst, strconv.FormatFloat(last_hop.Worst, 'f', 3, 64))
}

func sendMetric(key, value string) {
	if NOOP {
		log.Infof("NOOP: Sending metric %s %s", key, value)
		return
	}
	err := Graphite.SimpleSend(key, value)
	if err != nil {
		log.Debugf("Error sending metric %s: %s", key, err)
	}
}

func parseBrokerUrls(brokerUrls string) []string {
	tokens := strings.Split(brokerUrls, ",")
	for i, url := range tokens {
		tokens[i] = strings.TrimSpace(url)
	}

	return tokens
}

func main() {
	kingpin.Version(PKG_VERSION)

	brokerUrls := kingpin.Flag("broker-urls", "Comman separated MQTT broker URLs").
		Required().Default("").OverrideDefaultFromEnvar("MQTT_URLS").String()

	cafile := kingpin.Flag("cafile", "CA certificate when using TLS (optional)").
		String()

	topic := kingpin.Flag("topic", "MQTT topic").
		Default("/metrics/mtr").String()

	graphiteHost := kingpin.Flag("graphiteHost", "Graphite host").
		Default("localhost").String()

	clientID := kingpin.Flag("clientid", "Use a custom MQTT client ID").String()

	graphitePort := kingpin.Flag("graphitePort", "Graphite port").
		Default("2003").Int()

	graphitePing := kingpin.Flag("graphitePing", "Try to reconnect to graphite every X seconds").
		Default("15").Int()

	insecure := kingpin.Flag("insecure", "Don't verify the server's certificate chain and host name.").
		Default("false").Bool()

	debug := kingpin.Flag("debug", "Print debugging messages").
		Default("false").Bool()

	VERBOSE = *(kingpin.Flag("verbose", "Print metrics being sent").
		Default("false").Bool())

	noop := kingpin.Flag("noop", "Print the metrics being sent instead of sending them").
		Default("false").Bool()

	kingpin.Parse()

	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	NOOP = *noop
	var err error

	if *cafile != "" {
		if _, err := os.Stat(*cafile); err != nil {
			log.Fatalf("Error reading CA certificate %s", err.Error())
			os.Exit(1)
		}
	}

	urlList := parseBrokerUrls(*brokerUrls)

	var gerr error
	Graphite, gerr = graphite.NewGraphite(*graphiteHost, *graphitePort)
	if gerr != nil {
		log.Warn("Error connecting to graphite")
		os.Exit(1)
	} else {
		log.Info("Connected to graphite")
		log.Debugf("Loaded Graphite connection: %#v", Graphite)
	}

	if *clientID == "" {
		*clientID, err = os.Hostname()
		if err != nil {
			log.Fatal("Can't get the hostname to use it as the ClientID, use --clientid option")
		}
	}
	log.Debugf("MQTT Client ID: %s", *clientID)

	for _, urlStr := range urlList {
		args := mqttc.Args{
			BrokerURLs:    []string{urlStr},
			ClientID:      *clientID,
			Topic:         *topic,
			TLSCACertPath: *cafile,
			TLSSkipVerify: *insecure,
		}

		uri, _ := url.Parse(urlStr)
		log.Debug("Starting mqttc client")
		c := mqttc.Subscribe(sendMetrics, &args)
		defer c.Disconnect(0)
		host := strings.Split(uri.Host, ":")[0]
		host = strings.Replace(host, ".", "_", -1)
	}

	// Try to reconnect every graphitePing sec if sending fails by sending
	// a fake ping metric
	// FIXME: better handled by the Graphite client
	go func() {
		for {
			time.Sleep(time.Duration(*graphitePing) * time.Second)
			err := Graphite.SimpleSend("ping metric", "")
			if err != nil {
				log.Warn("Ping metric failed, trying to reconnect")
				err = Graphite.Connect()
				if err != nil {
					log.Error("Reconnecting to graphite failed")
				} else {
					log.Info("Reconnected to graphite")
				}
			}
		}
	}()

	// wait endlessly
	var loop chan bool
	loop <- true
}
