package godb

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/btm6084/gojson"
	"github.com/btm6084/utilities/metrics"
)

var (
	stripQueryRE = regexp.MustCompile(`^[^?]+`)
)

// JSONApi is an implementation of the Fetcher and JSONFetcher interfaces()
type JSONApi struct {
	baseURL  string
	pingPath string
	client   http.Client
}

// NewJSONApi configures and returns a usable JSONApi with a baseURL and pingPath.
// baseURL should include an appropriate scheme and hostname.
// pingPath is the path relative to the baseURL that can be used to verify the API is reachable;
// pingPath should always return an HTTP 200 OK status
func NewJSONApi(baseURL, pingPath string, requestTimeout time.Duration) *JSONApi {
	baseURL = strings.TrimRight(baseURL, "/")
	pingPath = strings.TrimLeft(pingPath, "/")

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	fetcher := &JSONApi{
		baseURL:  baseURL,
		pingPath: pingPath,
		client: http.Client{
			Timeout:   requestTimeout,
			Transport: t,
		},
	}

	return fetcher
}

func (h *JSONApi) requestURL(path string) string {
	return strings.Join([]string{
		strings.TrimRight(h.baseURL, "/"),
		strings.TrimLeft(path, "/"),
	}, "/")
}

func readResponse(res *http.Response) ([]byte, error) {
	// Decompress gzip content
	var err error
	var rawBody io.ReadCloser
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		rawBody, err = gzip.NewReader(res.Body)
		if err != nil {
			rawBody = res.Body
		}
	default:
		rawBody = res.Body
	}

	b, err := ioutil.ReadAll(rawBody)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (h *JSONApi) Ping(ctx context.Context) error {
	res, err := h.client.Get(h.requestURL(h.pingPath))
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid API ping status: %d %s", res.StatusCode, res.Status)
	}

	return nil
}

// FetchJSON makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (h *JSONApi) FetchJSON(ctx context.Context, requestURI string, args ...interface{}) ([]byte, error) {
	return h.FetchJSONWithMetrics(ctx, &metrics.NoOp{}, requestURI, args...)
}

// FetchJSONWithMetrics makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (h *JSONApi) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, requestURI string, args ...interface{}) ([]byte, error) {
	r.SetDBMeta(h.baseURL, stripQueryRE.FindString(requestURI), "GET")

	href := h.requestURL(fmt.Sprintf(requestURI, args...))

	req, err := http.NewRequest("GET", href, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	end := r.DatabaseSegment(h.baseURL, requestURI, args...)
	res, err := h.client.Do(req)
	end()
	if err != nil {
		return nil, err
	}

	b, err := readResponse(res)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Fetch makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (h *JSONApi) Fetch(ctx context.Context, requestURI string, container interface{}, args ...interface{}) error {
	return h.FetchWithMetrics(ctx, &metrics.NoOp{}, requestURI, container, args...)
}

// Fetch makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (h *JSONApi) FetchWithMetrics(ctx context.Context, r metrics.Recorder, requestURI string, container interface{}, args ...interface{}) error {
	b, err := h.FetchJSONWithMetrics(ctx, r, requestURI, args...)
	if err != nil {
		return err
	}

	err = gojson.Unmarshal(b, &container)
	if err != nil {
		return err
	}

	return nil
}
