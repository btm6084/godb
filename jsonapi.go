package godb

import (
	"compress/gzip"
	"context"
	"errors"
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
	ErrNotFound  = errors.New("not found")
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

func (j *JSONApi) requestURL(path string) string {
	if j == nil {
		return ""
	}

	return strings.Join([]string{
		strings.TrimRight(j.baseURL, "/"),
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
func (j *JSONApi) Ping(ctx context.Context) error {
	if j == nil {
		return ErrEmptyObject
	}

	res, err := j.client.Get(j.requestURL(j.pingPath))
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
func (j *JSONApi) FetchJSON(ctx context.Context, requestURI string, args ...interface{}) ([]byte, error) {
	if j == nil {
		return nil, ErrEmptyObject
	}

	return j.FetchJSONWithMetrics(ctx, &metrics.NoOp{}, requestURI, args...)
}

// FetchJSONWithMetrics makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (j *JSONApi) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, requestURI string, args ...interface{}) ([]byte, error) {
	if j == nil {
		return nil, ErrEmptyObject
	}

	r.SetDBMeta(j.baseURL, stripQueryRE.FindString(requestURI), "GET")

	href := j.requestURL(fmt.Sprintf(requestURI, args...))

	req, err := http.NewRequest("GET", href, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	end := r.DatabaseSegment(j.baseURL, requestURI, args...)
	res, err := j.client.Do(req)
	end()
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if res.StatusCode/100 > 3 {
		return nil, fmt.Errorf("godb.JSONApi: invalid status code %d (%s)", res.StatusCode, res.Status)
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
func (j *JSONApi) Fetch(ctx context.Context, requestURI string, container interface{}, args ...interface{}) error {
	if j == nil {
		return ErrEmptyObject
	}

	return j.FetchWithMetrics(ctx, &metrics.NoOp{}, requestURI, container, args...)
}

// Fetch makes a request to baseURL/requestURI.
// RequestURI should be the full relative path + query string.
// Any args passed in will be passed to fmt.Sprintf(requestURI, args...)
func (j *JSONApi) FetchWithMetrics(ctx context.Context, r metrics.Recorder, requestURI string, container interface{}, args ...interface{}) error {
	if j == nil {
		return ErrEmptyObject
	}

	b, err := j.FetchJSONWithMetrics(ctx, r, requestURI, args...)
	if err != nil {
		return err
	}

	err = gojson.Unmarshal(b, &container)
	if err != nil {
		return err
	}

	return nil
}
