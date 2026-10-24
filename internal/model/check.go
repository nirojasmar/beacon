package model

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Check represents a monitoring check definition.
type Check struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Type            string    `json:"type"` // http, tcp, dns
	Target          string    `json:"target"`
	IntervalSeconds int       `json:"interval_seconds"`
	TimeoutSeconds  int       `json:"timeout_seconds"`
	Expect          Expect    `json:"expect,omitempty"`
	RecordType      string    `json:"record_type,omitempty"`
	FailureThreshold int      `json:"failure_threshold"`
	SuccessThreshold int      `json:"success_threshold"`
	Enabled         bool      `json:"enabled"`
	Tags            []string  `json:"tags"`
	Source          string    `json:"source"` // manual, discovered
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Expect holds expected response conditions.
type Expect struct {
	StatusMin int `json:"status_min,omitempty"`
	StatusMax int `json:"status_max,omitempty"`
	BodyContains string `json:"body_contains,omitempty"`
}

// Result represents a probe result.
type Result struct {
	RunID      string       `json:"run_id,omitempty"`
	Timestamp time.Time    `json:"ts"`
	OK        bool         `json:"ok"`
	LatencyMs int          `json:"latency_ms"`
	HTTPStatus int         `json:"http_status,omitempty"`
	Error     string       `json:"error,omitempty"`
	Type      string       `json:"type"` // http, tcp, dns
}

// State represents the current state of a check.
type State struct {
	CheckID          string    `json:"check_id"`
	Status           string    `json:"status"` // unknown, up, down
	ConsecutiveFailures int    `json:"consecutive_failures"`
	ConsecutiveSuccesses int   `json:"consecutive_successes"`
	ChangeSeq        int64     `json:"change_seq"`
	LastChangeAt     time.Time `json:"last_change_at"`
	LastResult       Result    `json:"last_result"`
	Recent           []Result  `json:"recent"` // last 30 results, newest first
	ProcessedRunIDs   []string  `json:"processed_run_ids,omitempty"`
	RecentEvents      []Event   `json:"recent_events,omitempty"`
}

// Event represents a state change event.
type Event struct {
	CheckID   string    `json:"check_id"`
	Type      string    `json:"type"` // state_change
	ChangeSeq int64     `json:"change_seq"`
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	RunID     string    `json:"run_id,omitempty"`
}

// NewCheck creates a new check with the given ID.
func NewCheck(id, name, typ, target string, intervalSeconds, timeoutSeconds int) *Check {
	return &Check{
		ID:              id,
		Name:            name,
		Type:            typ,
		Target:          target,
		IntervalSeconds: intervalSeconds,
		TimeoutSeconds:  timeoutSeconds,
		FailureThreshold: 3,
		SuccessThreshold: 1,
		Enabled:         true,
		Tags:            []string{},
		Source:          "manual",
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
}

// NewState creates a new state for a check.
func NewState(checkID string) *State {
	return &State{
		CheckID:         checkID,
		Status:          "unknown",
		ConsecutiveFailures: 0,
		ConsecutiveSuccesses: 0,
		ChangeSeq:       0,
		LastChangeAt:    time.Now(),
	}
}

// NewEvent creates a new event.
func NewEvent(checkID string, typ, status string, changeSeq int64) *Event {
	return &Event{
		CheckID:   checkID,
		Type:      typ,
		ChangeSeq: changeSeq,
		Status:    status,
		Message:   "",
		Timestamp: time.Now(),
	}
}

func (s *State) ApplyResult(check *Check, result Result) (*Event, bool) {
	for _, id := range s.ProcessedRunIDs {
		if result.RunID != "" && id == result.RunID {
			for i := range s.RecentEvents {
				if s.RecentEvents[i].RunID == result.RunID {
					return &s.RecentEvents[i], false
				}
			}
			return nil, false
		}
	}
	if result.RunID != "" {
		s.ProcessedRunIDs = append(s.ProcessedRunIDs, result.RunID)
		if len(s.ProcessedRunIDs) > 100 {
			s.ProcessedRunIDs = s.ProcessedRunIDs[len(s.ProcessedRunIDs)-100:]
		}
	}

	s.LastResult = result
	s.Recent = append([]Result{result}, s.Recent...)
	if len(s.Recent) > 30 {
		s.Recent = s.Recent[:30]
	}
	if result.OK {
		s.ConsecutiveSuccesses++
		s.ConsecutiveFailures = 0
		if s.Status != "up" && s.ConsecutiveSuccesses >= check.SuccessThreshold {
			event := s.changeStatus("up")
			event.RunID = result.RunID
			s.rememberEvent(*event)
			return event, true
		}
	} else {
		s.ConsecutiveFailures++
		s.ConsecutiveSuccesses = 0
		if s.Status != "down" && s.ConsecutiveFailures >= check.FailureThreshold {
			event := s.changeStatus("down")
			event.RunID = result.RunID
			s.rememberEvent(*event)
			return event, true
		}
	}
	return nil, true
}

func (s *State) rememberEvent(event Event) {
	s.RecentEvents = append(s.RecentEvents, event)
	if len(s.RecentEvents) > 30 {
		s.RecentEvents = s.RecentEvents[len(s.RecentEvents)-30:]
	}
}

func (s *State) changeStatus(status string) *Event {
	s.Status = status
	s.ChangeSeq++
	s.LastChangeAt = time.Now()
	return NewEvent(s.CheckID, "state_change", status, s.ChangeSeq)
}

// Validate checks if a check is valid.
func (c *Check) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return invalidField("name", fmt.Errorf("name is required"))
	}
	if len(c.Name) > 200 {
		return invalidField("name", fmt.Errorf("name must be at most 200 characters"))
	}
	if c.Type != "http" && c.Type != "tcp" && c.Type != "dns" {
		return invalidField("type", ErrInvalidType)
	}
	if strings.TrimSpace(c.Target) == "" {
		return invalidField("target", fmt.Errorf("target is required"))
	}

	if c.IntervalSeconds < 10 {
		return invalidField("interval_seconds", ErrIntervalTooShort)
	}

	if c.TimeoutSeconds < 1 || c.TimeoutSeconds > 30 {
		return invalidField("timeout_seconds", ErrTimeoutOutOfRange)
	}

	if c.TimeoutSeconds >= c.IntervalSeconds {
		return invalidField("timeout_seconds", ErrTimeoutGreaterOrEqualInterval)
	}
	if c.FailureThreshold < 1 {
		return invalidField("failure_threshold", fmt.Errorf("failure_threshold must be positive"))
	}
	if c.SuccessThreshold < 1 {
		return invalidField("success_threshold", fmt.Errorf("success_threshold must be positive"))
	}
	if err := c.Expect.Validate(); err != nil {
		return err
	}
	switch c.Type {
	case "http":
		target, err := url.ParseRequestURI(c.Target)
		if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" {
			return invalidField("target", fmt.Errorf("target must be a valid http or https URL"))
		}
	case "tcp":
		host, port, err := net.SplitHostPort(c.Target)
		if err != nil || host == "" {
			return invalidField("target", fmt.Errorf("target must be a host:port pair"))
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return invalidField("target", fmt.Errorf("TCP target port must be between 1 and 65535"))
		}
	case "dns":
		if strings.ContainsAny(c.Target, " /\\") {
			return invalidField("target", fmt.Errorf("target must be a DNS hostname"))
		}
		if c.RecordType != "" && !strings.EqualFold(c.RecordType, "A") &&
			!strings.EqualFold(c.RecordType, "AAAA") && !strings.EqualFold(c.RecordType, "CNAME") {
			return invalidField("record_type", fmt.Errorf("record_type must be A, AAAA, or CNAME"))
		}
	}

	return nil
}

// ValidateExpect checks if the expected values are valid.
func (e *Expect) Validate() error {
	if e.StatusMin != 0 && (e.StatusMin < 100 || e.StatusMin > 599) {
		return invalidField("expect.status_min", ErrInvalidStatusRange)
	}
	if e.StatusMax != 0 && (e.StatusMax < 100 || e.StatusMax > 599) {
		return invalidField("expect.status_max", ErrInvalidStatusRange)
	}

	if e.StatusMin > e.StatusMax && e.StatusMin != 0 && e.StatusMax != 0 {
		return invalidField("expect.status_min", ErrInvalidStatusRange)
	}

	return nil
}
