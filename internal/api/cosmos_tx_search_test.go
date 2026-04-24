package api

import "testing"

func TestParseTxSearchFilter(t *testing.T) {
	filter, supported, err := parseTxSearchFilter("message.action='/cosmos.bank.v1beta1.MsgSend' AND transfer.recipient='terra1abc' AND tx.height=123", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !supported {
		t.Fatalf("expected supported query")
	}
	if filter.height == nil || *filter.height != 123 {
		t.Fatalf("expected height 123, got %#v", filter.height)
	}
	if len(filter.eventConditions) != 2 {
		t.Fatalf("expected 2 event conditions, got %d", len(filter.eventConditions))
	}
	if filter.eventConditions[0].eventType != "message" || filter.eventConditions[0].attrKey != "action" {
		t.Fatalf("unexpected first condition: %#v", filter.eventConditions[0])
	}
}

func TestParseTxSearchFilterEventsDeprecated(t *testing.T) {
	filter, supported, err := parseTxSearchFilter("", []string{"message.sender='terra1xyz'", "tx.hash='ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !supported {
		t.Fatalf("expected supported deprecated events query")
	}
	if filter.hash == "" {
		t.Fatalf("expected tx hash filter")
	}
	if len(filter.eventConditions) != 1 {
		t.Fatalf("expected 1 event condition, got %d", len(filter.eventConditions))
	}
}

func TestParseTxSearchFilterUnsupported(t *testing.T) {
	_, supported, err := parseTxSearchFilter("message.action='x' OR message.sender='y'", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if supported {
		t.Fatalf("expected unsupported query")
	}
}

func TestTxLocationKey(t *testing.T) {
	key := txLocationKey(txLocation{Height: 12, IndexInBlock: 3})
	if key != "12-3" {
		t.Fatalf("unexpected key: %s", key)
	}
}
