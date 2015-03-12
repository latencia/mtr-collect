package mqttc

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"git.eclipse.org/gitroot/paho/org.eclipse.paho.mqtt.golang.git"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"time"
)

type Args struct {
	Topic         string
	ClientID      string
	BrokerURLs    []string
	TLSCACertPath string
	TLSSkipVerify bool
}

// tcp://user:password@host:port
func PushMsg(msg string, args *Args) error {

	opts := mqtt.NewClientOptions()
	opts.SetCleanSession(true)
	opts.SetWriteTimeout(10 * time.Second)

	opts.SetClientId(args.ClientID)
	for _, broker := range args.BrokerURLs {
		uri, err := url.Parse(broker)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing broker url (ignored): %s\n", broker)
			continue
		}
		if uri.Scheme == "ssl" {
			tlsconfig := newTlsConfig(args.TLSCACertPath, args.TLSSkipVerify)
			if !isTLSOK(*uri, tlsconfig) {
				continue
			}
			opts.SetTlsConfig(tlsconfig)
		}
		opts.AddBroker(broker)
	}

	client := mqtt.NewClient(opts)
	_, err := client.Start()
	if err != nil {
		return errors.New("Connection to the broker(s) failed: " + err.Error())
	}
	defer client.Disconnect(0)

	<-client.Publish(mqtt.QOS_ONE, args.Topic, msg)

	return nil
}

func Subscribe(handler mqtt.MessageHandler, args *Args) *mqtt.MqttClient {
	opts := mqtt.NewClientOptions()

	for _, broker := range args.BrokerURLs {
		uri, err := url.Parse(broker)
		if err != nil {
			log.Errorf("Error parsing broker url (ignored): %s\n", broker)
			continue
		}
		if uri.Scheme == "ssl" {
			tlsconfig := newTlsConfig(args.TLSCACertPath, args.TLSSkipVerify)
			if !isTLSOK(*uri, tlsconfig) {
				continue
			}
			opts.SetTlsConfig(tlsconfig)
		}
		log.Debug("Adding broker", broker)
		opts.AddBroker(broker)
	}

	opts.SetClientId(args.ClientID)
	opts.SetDefaultPublishHandler(handler)

	client := mqtt.NewClient(opts)
	_, err := client.Start()
	if err != nil {
		log.Fatal("Connection to the broker(s) failed: " + err.Error())
	}

	filter, e := mqtt.NewTopicFilter(args.Topic, byte(mqtt.QOS_ZERO))
	if e != nil {
		log.Panic(e)
	}

	if rectp, err := client.StartSubscription(nil, filter); err != nil {
		log.Panic(err)
	} else {
		<-rectp
	}

	log.Debug("Subscription to the brokers started")
	return client
}

func newTlsConfig(cacertPath string, verify bool) *tls.Config {
	if cacertPath == "" {
		return &tls.Config{}
	}

	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile(cacertPath)
	if err != nil {
		panic("Error reading CA certificate from " + cacertPath)
	}

	certpool.AppendCertsFromPEM(pemCerts)

	return &tls.Config{
		RootCAs:            certpool,
		InsecureSkipVerify: verify,
	}
}

// Test SSL connections to the brokers because the current
// paho mqtt client implementation returns a generic error message
// hard to debug.
func isTLSOK(uri url.URL, config *tls.Config) bool {
	r := regexp.MustCompile(".*:.*")
	if !r.MatchString(uri.Host) {
		uri.Host += ":8883"
	}

	_, err := tls.Dial("tcp", uri.Host, config)

	if err != nil {
		log.Warnf("Ignoring broker %s: %s", uri.String(), err)
		return false
	}

	return true
}
