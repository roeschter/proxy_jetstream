// Package main implements a NATS JetStream "fake domain" consumer proxy.
//
// The proxy listens on a fake JetStream API namespace of the form
//
//	$JS.<fakeDomain>.API.CONSUMER.<op>.<stream>.<consumer>
//
// and translates the request to the real (no-domain) JetStream API
//
//	$JS.API.CONSUMER.<op>.<stream>.<translatedConsumer>
//
// Each client application is expected to use its own fake domain so that
// consumer-name translation provides per-tenant security isolation.
//
// Only the client->server direction is rewritten. Reply subjects are left
// untouched so that responses (and especially NEXT message deliveries) flow
// directly from the JetStream server back to the originating client.
//
// Proxied operations:
//
//	CONSUMER.CREATE         - two-way proxy (subject + request body + response body)
//	CONSUMER.INFO           - two-way proxy (subject + response body)
//	CONSUMER.MSG.NEXT       - one-way proxy (subject only; reply left untouched)
//
// Consumer-name translation is pluggable via the TranslateFunc callbacks. The
// default forward translation prefixes the consumer name with the fake domain
// (e.g. "tenant1_orders"); the default backward translation strips that prefix.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

// TranslateFunc maps a consumer name in one direction (forward = client->server,
// backward = server->client). It is invoked for every proxied request/response
// and must be deterministic.
type TranslateFunc func(consumerName string) string

// ConsumerProxy translates consumer-API requests from a fake JetStream domain
// onto the real (no-domain) JetStream API on the same NATS connection/account.
type ConsumerProxy struct {
	nc                *nats.Conn
	fakeDomain        string
	forwardTranslate  TranslateFunc
	backwardTranslate TranslateFunc
	requestTimeout    time.Duration
	subs              []*nats.Subscription
}

// DefaultForwardTranslate returns a TranslateFunc that prefixes the consumer
// name with the fake domain ("<fakeDomain>_<consumer>"). This is the default
// implementation used when the caller does not supply a custom forward
// translation.
func DefaultForwardTranslate(fakeDomain string) TranslateFunc {
	prefix := fakeDomain + "_"
	return func(consumerName string) string {
		if consumerName == "" || strings.HasPrefix(consumerName, prefix) {
			return consumerName
		}
		return prefix + consumerName
	}
}

// DefaultBackwardTranslate is the inverse of DefaultForwardTranslate: it strips
// the "<fakeDomain>_" prefix from the consumer name if present.
func DefaultBackwardTranslate(fakeDomain string) TranslateFunc {
	prefix := fakeDomain + "_"
	return func(consumerName string) string {
		return strings.TrimPrefix(consumerName, prefix)
	}
}

// NewConsumerProxy builds a proxy bound to the given NATS connection and fake
// JetStream domain. Passing nil for either translate callback installs the
// default prefix-based implementation.
func NewConsumerProxy(nc *nats.Conn, fakeDomain string, forward, backward TranslateFunc) *ConsumerProxy {
	if forward == nil {
		forward = DefaultForwardTranslate(fakeDomain)
	}
	if backward == nil {
		backward = DefaultBackwardTranslate(fakeDomain)
	}
	return &ConsumerProxy{
		nc:                nc,
		fakeDomain:        fakeDomain,
		forwardTranslate:  forward,
		backwardTranslate: backward,
		requestTimeout:    10 * time.Second,
	}
}

// SetRequestTimeout overrides the default upstream request timeout used by the
// two-way (CREATE / INFO) proxies.
func (p *ConsumerProxy) SetRequestTimeout(d time.Duration) {
	p.requestTimeout = d
}

// Start installs subscriptions for the three proxied operations. The proxy
// runs entirely on the NATS connection's delivery goroutines; each callback
// dispatches the actual upstream call on a fresh goroutine so a single slow
// request cannot stall the subscription.
func (p *ConsumerProxy) Start() error {
	createSubj := fmt.Sprintf("$JS.%s.API.CONSUMER.CREATE.*.*", p.fakeDomain)
	infoSubj := fmt.Sprintf("$JS.%s.API.CONSUMER.INFO.*.*", p.fakeDomain)
	nextSubj := fmt.Sprintf("$JS.%s.API.CONSUMER.MSG.NEXT.*.*", p.fakeDomain)

	for _, binding := range []struct {
		subject string
		handler nats.MsgHandler
	}{
		{createSubj, func(m *nats.Msg) { go p.handleCreate(m) }},
		{infoSubj, func(m *nats.Msg) { go p.handleInfo(m) }},
		{nextSubj, p.handleNext}, // synchronous: cheap publish, preserves ordering
	} {
		sub, err := p.nc.Subscribe(binding.subject, binding.handler)
		if err != nil {
			p.Stop()
			return fmt.Errorf("subscribe %s: %w", binding.subject, err)
		}
		p.subs = append(p.subs, sub)
	}
	return nil
}

// Stop unsubscribes from all proxied subjects. Safe to call multiple times.
func (p *ConsumerProxy) Stop() {
	for _, s := range p.subs {
		_ = s.Unsubscribe()
	}
	p.subs = nil
}

// ---------------------------------------------------------------------------
// CONSUMER.CREATE - two-way proxy
// ---------------------------------------------------------------------------
//
// Incoming subject: $JS.<fakeDomain>.API.CONSUMER.CREATE.<stream>.<consumer>
// Upstream subject: $JS.API.CONSUMER.CREATE.<stream>.<translatedConsumer>
//
// The request body (a JSON ConsumerConfig wrapper) also carries the consumer
// name in its "name" / "config.name" / "config.durable_name" fields and must
// be rewritten forward. The response body carries the consumer info and is
// rewritten backward before being returned to the client.
func (p *ConsumerProxy) handleCreate(msg *nats.Msg) {
	stream, consumer, ok := parseStreamConsumer(msg.Subject, 5)
	if !ok {
		log.Printf("[create] malformed subject: %s", msg.Subject)
		return
	}
	translated := p.forwardTranslate(consumer)

	body, err := rewriteConsumerName(msg.Data, consumer, translated)
	if err != nil {
		log.Printf("[create] request body rewrite failed: %v (forwarding original)", err)
		body = msg.Data
	}

	upstream := fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s", stream, translated)
	resp, err := p.nc.Request(upstream, body, p.requestTimeout)
	if err != nil {
		log.Printf("[create] upstream request to %s failed: %v", upstream, err)
		return
	}

	respBody, err := rewriteConsumerName(resp.Data, translated, consumer)
	if err != nil {
		log.Printf("[create] response body rewrite failed: %v (returning original)", err)
		respBody = resp.Data
	}

	p.respond(msg, respBody)
}

// ---------------------------------------------------------------------------
// CONSUMER.INFO - two-way proxy
// ---------------------------------------------------------------------------
//
// Incoming subject: $JS.<fakeDomain>.API.CONSUMER.INFO.<stream>.<consumer>
// Upstream subject: $JS.API.CONSUMER.INFO.<stream>.<translatedConsumer>
//
// INFO has no request body (or an empty body) but the response carries the
// translated consumer name which must be rewritten back.
func (p *ConsumerProxy) handleInfo(msg *nats.Msg) {
	stream, consumer, ok := parseStreamConsumer(msg.Subject, 5)
	if !ok {
		log.Printf("[info] malformed subject: %s", msg.Subject)
		return
	}
	translated := p.forwardTranslate(consumer)

	upstream := fmt.Sprintf("$JS.API.CONSUMER.INFO.%s.%s", stream, translated)
	resp, err := p.nc.Request(upstream, msg.Data, p.requestTimeout)
	if err != nil {
		log.Printf("[info] upstream request to %s failed: %v", upstream, err)
		return
	}

	respBody, err := rewriteConsumerName(resp.Data, translated, consumer)
	if err != nil {
		log.Printf("[info] response body rewrite failed: %v (returning original)", err)
		respBody = resp.Data
	}

	p.respond(msg, respBody)
}

// ---------------------------------------------------------------------------
// CONSUMER.MSG.NEXT - one-way proxy
// ---------------------------------------------------------------------------
//
// Incoming subject: $JS.<fakeDomain>.API.CONSUMER.MSG.NEXT.<stream>.<consumer>
// Upstream subject: $JS.API.CONSUMER.MSG.NEXT.<stream>.<translatedConsumer>
//
// The reply subject is forwarded UNCHANGED so the JetStream server delivers
// messages straight to the original requester. No response is post-processed
// by the proxy.
func (p *ConsumerProxy) handleNext(msg *nats.Msg) {
	stream, consumer, ok := parseStreamConsumer(msg.Subject, 6)
	if !ok {
		log.Printf("[next] malformed subject: %s", msg.Subject)
		return
	}
	translated := p.forwardTranslate(consumer)

	upstream := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.%s.%s", stream, translated)
	out := &nats.Msg{
		Subject: upstream,
		Reply:   msg.Reply, // preserved verbatim - this is the whole point
		Header:  msg.Header,
		Data:    msg.Data,
	}
	if err := p.nc.PublishMsg(out); err != nil {
		log.Printf("[next] forward to %s failed: %v", upstream, err)
	}
}

// respond publishes a reply if the request had a reply subject.
func (p *ConsumerProxy) respond(msg *nats.Msg, data []byte) {
	if msg.Reply == "" {
		return
	}
	if err := p.nc.Publish(msg.Reply, data); err != nil {
		log.Printf("reply publish to %s failed: %v", msg.Reply, err)
	}
}

// parseStreamConsumer extracts the stream and consumer names from the trailing
// two tokens of a JetStream API subject. opIndex is the index of the operation
// verb (CREATE/INFO/NEXT) so that stream lives at opIndex+1 and consumer at
// opIndex+2.
func parseStreamConsumer(subject string, opIndex int) (stream, consumer string, ok bool) {
	tokens := strings.Split(subject, ".")
	if len(tokens) < opIndex+3 {
		return "", "", false
	}
	stream = tokens[opIndex+1]
	consumer = tokens[opIndex+2]
	if stream == "" || consumer == "" {
		return "", "", false
	}
	return stream, consumer, true
}

// rewriteConsumerName rewrites the consumer-name fields of a JetStream API
// JSON envelope, replacing every occurrence of `from` with `to`. It is
// tolerant of empty bodies and non-JSON payloads (returns the input unchanged
// for empty bodies; returns an error for malformed JSON).
//
// Fields rewritten (when present and equal to `from`):
//   - top-level "name"
//   - top-level "durable_name"
//   - nested "config.name"
//   - nested "config.durable_name"
//
// Stream-related fields ("stream_name") are deliberately left alone.
func rewriteConsumerName(data []byte, from, to string) ([]byte, error) {
	if len(data) == 0 || from == to {
		return data, nil
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return data, err
	}
	replaceNameFields(obj, from, to)
	if cfg, ok := obj["config"].(map[string]any); ok {
		replaceNameFields(cfg, from, to)
	}
	return json.Marshal(obj)
}

func replaceNameFields(obj map[string]any, from, to string) {
	for _, key := range []string{"name", "durable_name"} {
		if v, ok := obj[key].(string); ok && v == from {
			obj[key] = to
		}
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

func main() {
	url := envOr("NATS_URL", nats.DefaultURL)
	fakeDomain := envOr("FAKE_JS_DOMAIN", "tenant1")

	nc, err := nats.Connect(url,
		nats.Name("domain-consumer-proxy/"+fakeDomain),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		log.Fatalf("nats connect %s: %v", url, err)
	}
	defer nc.Drain()

	proxy := NewConsumerProxy(nc, fakeDomain, nil, nil)
	if err := proxy.Start(); err != nil {
		log.Fatalf("proxy start: %v", err)
	}
	defer proxy.Stop()

	log.Printf("consumer proxy running: fake domain %q -> real JetStream API on %s", fakeDomain, url)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutting down")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
