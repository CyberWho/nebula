package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
	"github.com/volatiletech/null/v8"

	pgmodels "github.com/dennis-tra/nebula-crawler/db/models/pg"
)

// JSONCrawl is a custom struct for JSON output that includes all metrics
// including the new paper-methodology metrics (Discv5Reachable, TcpDialable, PingResponded)
type JSONCrawl struct {
	State           string     `json:"state"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	UpdatedAt       time.Time  `json:"updated_at"`
	CreatedAt       time.Time  `json:"created_at"`
	CrawledPeers    *int       `json:"crawled_peers,omitempty"`
	DialablePeers   *int       `json:"dialable_peers,omitempty"`
	UndialablePeers *int       `json:"undialable_peers,omitempty"`
	RemainingPeers  *int       `json:"remaining_peers,omitempty"`
	Discv5Reachable *int       `json:"discv5_reachable,omitempty"`
	TcpDialable     *int       `json:"tcp_dialable,omitempty"`
	PingResponded   *int       `json:"ping_responded,omitempty"`
	Version         string     `json:"version"`
}

type JSONClient struct {
	out string

	prefix string

	visitsFile       *os.File
	neighborsFile    *os.File
	visitEncoder     *json.Encoder
	neighborsEncoder *json.Encoder

	// ... TODO
	routingTables map[peer.ID]struct {
		neighbors []peer.ID
		errorBits uint16
	}

	crawlMu sync.Mutex
	crawl   *JSONCrawl
}

func NewJSONClient(out string) (*JSONClient, error) {
	log.WithField("out", out).Infoln("Initializing JSON client")

	if err := os.MkdirAll(out, 0o755); err != nil {
		return nil, fmt.Errorf("make json out directory: %w", err)
	}
	prefix := path.Join(out, time.Now().Format("2006-01-02T15:04"))

	vf, err := os.Create(prefix + "_visits.ndjson")
	if err != nil {
		return nil, fmt.Errorf("create visits file: %w", err)
	}

	nf, err := os.Create(prefix + "_neighbors.ndjson")
	if err != nil {
		return nil, fmt.Errorf("create neighbors file: %w", err)
	}

	client := &JSONClient{
		out:              out,
		prefix:           prefix,
		visitsFile:       vf,
		neighborsFile:    nf,
		visitEncoder:     json.NewEncoder(vf),
		neighborsEncoder: json.NewEncoder(nf),
		routingTables: make(map[peer.ID]struct {
			neighbors []peer.ID
			errorBits uint16
		}),
	}

	return client, nil
}

func (c *JSONClient) InitCrawl(ctx context.Context, version string) (err error) {
	c.crawlMu.Lock()
	defer c.crawlMu.Unlock()

	defer func() {
		if err != nil {
			c.crawl = nil
		}
	}()

	if c.crawl != nil {
		return fmt.Errorf("crawl already initialized")
	}

	now := time.Now()

	c.crawl = &JSONCrawl{
		State:     pgmodels.CrawlStateStarted,
		StartedAt: now,
		Version:   version,
		UpdatedAt: now,
		CreatedAt: now,
	}

	data, err := json.MarshalIndent(c.crawl, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal crawl json: %w", err)
	}

	if err = os.WriteFile(c.prefix+"_crawl.json", data, 0o644); err != nil {
		return fmt.Errorf("write crawl json: %w", err)
	}

	return nil
}

func (c *JSONClient) SealCrawl(ctx context.Context, args *SealCrawlArgs) (err error) {
	c.crawlMu.Lock()
	defer c.crawlMu.Unlock()

	original := *c.crawl
	defer func() {
		// roll back in case of an error
		if err != nil {
			c.crawl = &original
		}
	}()

	now := time.Now()
	c.crawl.UpdatedAt = now
	c.crawl.CrawledPeers = &args.Crawled
	c.crawl.DialablePeers = &args.Dialable
	c.crawl.UndialablePeers = &args.Undialable
	c.crawl.RemainingPeers = &args.Remaining
	c.crawl.Discv5Reachable = &args.Discv5Reachable
	c.crawl.TcpDialable = &args.TcpDialable
	c.crawl.PingResponded = &args.PingResponded
	c.crawl.State = string(args.State)
	c.crawl.FinishedAt = &now

	data, err := json.MarshalIndent(c.crawl, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal crawl json: %w", err)
	}

	if err = os.WriteFile(c.prefix+"_crawl.json", data, 0o644); err != nil {
		return fmt.Errorf("write crawl json: %w", err)
	}

	return nil
}

func (c *JSONClient) QueryBootstrapPeers(ctx context.Context, limit int) ([]peer.AddrInfo, error) {
	return []peer.AddrInfo{}, nil
}

func (c *JSONClient) InsertCrawlProperties(ctx context.Context, properties map[string]map[string]int) error {
	data, err := json.MarshalIndent(properties, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal properties json: %w", err)
	}

	if err = os.WriteFile(c.prefix+"_crawl_properties.json", data, 0o644); err != nil {
		return fmt.Errorf("write properties json: %w", err)
	}

	return nil
}

type JSONVisit struct {
	PeerID          peer.ID
	Maddrs          []ma.Multiaddr
	FilteredMaddrs  []ma.Multiaddr
	ListenMaddrs    []ma.Multiaddr
	ConnectMaddr    ma.Multiaddr
	Protocols       []string
	AgentVersion    string
	ConnectDuration string
	CrawlDuration   string
	VisitStartedAt  time.Time
	VisitEndedAt    time.Time
	ConnectErrorStr string
	CrawlErrorStr   string
	PingErrorStr    string
	Properties      null.JSON
}

func (c *JSONClient) InsertVisit(ctx context.Context, args *VisitArgs) error {
	data := JSONVisit{
		PeerID:          args.PeerID,
		Maddrs:          slices.Concat(args.DialMaddrs),
		FilteredMaddrs:  args.FilteredMaddrs,
		ListenMaddrs:    args.ListenMaddrs,
		ConnectMaddr:    args.ConnectMaddr,
		Protocols:       args.Protocols,
		AgentVersion:    args.AgentVersion,
		ConnectDuration: args.ConnectDuration.String(),
		CrawlDuration:   args.CrawlDuration.String(),
		VisitStartedAt:  args.VisitStartedAt,
		VisitEndedAt:    args.VisitEndedAt,
		ConnectErrorStr: args.ConnectErrorStr,
		CrawlErrorStr:   args.CrawlErrorStr,
		PingErrorStr:    args.PingErrorStr,
		Properties:      null.JSONFrom(args.Properties),
	}

	if len(args.Neighbors) > 0 || args.ErrorBits != 0 {
		if err := c.InsertNeighbors(ctx, args.PeerID, args.Neighbors, args.ErrorBits); err != nil {
			return fmt.Errorf("persiting neighbor information: %w", err)
		}
	}

	if err := c.visitEncoder.Encode(data); err != nil {
		return fmt.Errorf("encoding visit: %w", err)
	}

	return nil
}

type JSONNeighbors struct {
	PeerID      peer.ID
	NeighborIDs []peer.ID
	ErrorBits   string
}

func (c *JSONClient) InsertNeighbors(ctx context.Context, peerID peer.ID, neighbors []peer.ID, errorBits uint16) error {
	data := JSONNeighbors{
		PeerID:      peerID,
		NeighborIDs: neighbors,
		ErrorBits:   fmt.Sprintf("%016b", errorBits),
	}

	if err := c.neighborsEncoder.Encode(data); err != nil {
		return fmt.Errorf("encoding visit: %w", err)
	}

	return nil
}

func (c *JSONClient) SelectPeersToProbe(ctx context.Context) ([]peer.AddrInfo, error) {
	return []peer.AddrInfo{}, nil
}

func (c *JSONClient) Flush(ctx context.Context) error {
	return nil
}

func (c *JSONClient) Close() error {
	err1 := c.visitsFile.Close()
	err2 := c.neighborsFile.Close()
	if err1 != nil && err2 != nil {
		return fmt.Errorf("failed closing JSON files: %w", fmt.Errorf("%s: %w (neighbors)", err1, err2))
	} else if err1 != nil {
		return fmt.Errorf("failed closing visits file: %w", err1)
	} else if err2 != nil {
		return fmt.Errorf("failed closing neighbors files: %w", err2)
	}

	return nil
}
