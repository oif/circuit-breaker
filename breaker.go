package circuit_breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrIsOpen                        = errors.New("breaker is open")
	ErrHalfOpenButExceedRequestLimit = errors.New("breaker is half open, but current request exceed limit")

	DefaultOpenStateExpiry            = time.Minute
	DefaultFailureThreshold     int64 = 100
	DefaultSuccessThreshold     int64 = 100
	DefaultGenerationInterval         = 10 * time.Second
	DefaultHalfOpenRequestLimit int64 = 200
)

type State string

type Event struct {
	When   time.Time
	From   State
	To     State
	Reason string
}

const (
	Open     State = "open"
	HalfOpen State = "half-open"
	Closed   State = "closed"

	ReasonManuallyReset         = "manually reset"
	ReasonOpenStateExpired      = "open state expired"
	ReasonReachThreshold        = "reach threshold"
	ReasonFailedOnHalfOpenState = "failed on half-open state"
)

type StateChangeNotificationFunc func(Event)
type HandleFunc func() (interface{}, error)
type ShouldTripFunc func(Counts) bool

type Option struct {
	GenerationInterval   time.Duration
	OpenStateExpiry      time.Duration
	FailureThreshold     int64
	SuccessThreshold     int64
	HalfOpenRequestLimit int64
	OnStateChange        StateChangeNotificationFunc
	shouldTrip           ShouldTripFunc
}

type Counts struct {
	Request int64
	Success int64
	Failure int64
}

func (c Counts) String() string {
	return fmt.Sprintf("request: %d, success: %d, failure: %d",
		c.Request, c.Success, c.Failure)
}

func (c *Counts) reset() {
	c.Request = 0
	c.Success = 0
	c.Failure = 0
}

func New(opt Option) *CircuitBreaker {
	cb := new(CircuitBreaker)

	if opt.OpenStateExpiry == 0 {
		opt.OpenStateExpiry = DefaultOpenStateExpiry
	}

	if opt.FailureThreshold == 0 {
		opt.FailureThreshold = DefaultFailureThreshold
	}

	if opt.SuccessThreshold == 0 {
		opt.SuccessThreshold = DefaultSuccessThreshold
	}

	if opt.GenerationInterval == 0 {
		opt.GenerationInterval = DefaultGenerationInterval
	}

	if opt.HalfOpenRequestLimit == 0 {
		opt.HalfOpenRequestLimit = DefaultHalfOpenRequestLimit
	}

	if opt.HalfOpenRequestLimit < opt.SuccessThreshold {
		panic("half-open request limit should greater than success threshold")
	}

	// Default trip function
	opt.shouldTrip = func(c Counts) bool {
		return c.Failure >= opt.FailureThreshold
	}

	cb.state = Closed
	cb.opt = opt
	cb.generation = time.Now()
	cb.moveToNextGeneration(cb.generation)

	return cb
}

type CircuitBreaker struct {
	opt        Option
	state      State
	generation time.Time
	mutex      sync.RWMutex
	counts     Counts
}

// Get current breaker state
func (cb *CircuitBreaker) State() State {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	return cb.state
}

// Reset breaker to initial state
func (cb *CircuitBreaker) Reset() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.changeState(time.Now(), Closed, ReasonManuallyReset)
}

func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// The main part of breaker which execute logical handlers
func (cb *CircuitBreaker) Do(handle HandleFunc) (interface{}, error) {
	gen, err := cb.postStartHook()
	if err != nil {
		return nil, err
	}
	defer func(g time.Time) {
		// For panic
		e := recover()
		if e != nil {
			// Once panic, regard as failed
			cb.preStopHook(g, false)
			panic(e)
		}
	}(gen)
	// Execute it
	resp, err := handle()
	cb.preStopHook(gen, err == nil)
	return resp, err
}

// Return generation to avoid execute handle until next generation
func (cb *CircuitBreaker) postStartHook() (time.Time, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	// Check state
	now := time.Now()
	tempState := cb.state
	switch tempState {
	case Open:
		// Open state expired, change to half open
		if now.After(cb.generation) {
			cb.changeState(now, HalfOpen, ReasonOpenStateExpired)
		}
	case HalfOpen:
		// Over success threshold, close breaker
		if cb.counts.Success >= cb.opt.SuccessThreshold {
			cb.changeState(now, Closed, ReasonReachThreshold)
		}
	case Closed:
		// over failure threshold, open the breaker
		if cb.opt.shouldTrip(cb.counts) {
			cb.changeState(now, Open, ReasonReachThreshold)
		}
	}

	// State never changed
	if now.After(cb.generation) {
		cb.moveToNextGeneration(now)
	}

	// Breaker is open, intercept all the requests
	if cb.state == Open {
		return cb.generation, ErrIsOpen
	} else if cb.state == HalfOpen && cb.counts.Request > cb.opt.HalfOpenRequestLimit {
		// Half open but exceed the request limit
		return cb.generation, ErrHalfOpenButExceedRequestLimit
	}

	cb.counts.Request++
	return cb.generation, nil
}

func (cb *CircuitBreaker) preStopHook(currentGeneration time.Time, handleSuccess bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	// Handled in previous generation should not count in this generation
	if currentGeneration != cb.generation {
		return
	}

	if !handleSuccess {
		cb.counts.Failure++
	} else {
		cb.counts.Success++
	}

	currentState := cb.state
	// Handle state transaction
	switch currentState {
	case HalfOpen:
		if !handleSuccess {
			// Open it if failed once
			cb.changeState(time.Now(), Open, ReasonFailedOnHalfOpenState)
		}
	}
}

// Change to new state, will move to next generation and notify preset onStateChange handler
func (cb *CircuitBreaker) changeState(now time.Time, newState State, reason string) {
	current := cb.state
	cb.state = newState
	cb.moveToNextGeneration(now)
	cb.opt.OnStateChange(Event{
		When:   now,
		From:   current,
		To:     newState,
		Reason: reason,
	})
}

// Reset counts
func (cb *CircuitBreaker) resetCounts() {
	cb.counts.reset()
}

// Move to nex generation according to current state
func (cb *CircuitBreaker) moveToNextGeneration(now time.Time) {
	// State degeneration
	cb.resetCounts()
	switch cb.state {
	case Open:
		cb.generation = now.Add(cb.opt.OpenStateExpiry)
	case HalfOpen, Closed:
		cb.generation = now.Add(cb.opt.GenerationInterval)
	}
}
