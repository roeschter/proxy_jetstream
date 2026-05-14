
# Projects

## Fake domain to consumer prefix proxy

The proxy listens on a fake JetStream API namespace of the form
* $JS.<fakeDomain>.API.CONSUMER.<op>.<stream>.<consumer>

and translates the request to the real (no-domain) JetStream API

* $JS.API.CONSUMER.<op>.<stream>.<translatedConsumer>

Each client application is expected to use its own fake domain so that consumer-name translation provides per-tenant security isolation.

Only the client->server direction is rewritten. Reply subjects are left untouched so that responses (and especially NEXT message deliveries) flow directly from the JetStream server back to the originating client.

Proxied operations:
* CONSUMER.CREATE         - two-way proxy (subject + request body + response body)
* CONSUMER.INFO           - two-way proxy (subject + response body)
* CONSUMER.MSG.NEXT       - one-way proxy (subject only; reply left untouched)

Consumer-name translation is pluggable via the TranslateFunc callbacks. The default forward translation prefixes the consumer name with the fake domain (e.g. "tenant1_orders"); the default backward translation strips that prefix.
