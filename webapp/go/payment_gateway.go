package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

func requestPaymentGatewayPostPayment(ctx context.Context, paymentGatewayURL string, token string, idempotencyKey string, param *paymentGatewayPostPaymentRequest) (retry int, result error) {
	b, err := json.Marshal(param)
	if err != nil {
		return 0, err
	}

	// Reuse the key and payload even when an accepted POST loses its response.
	for {
		if err := ctx.Err(); err != nil {
			return retry, err
		}
		retryable, err := func() (bool, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
			if err != nil {
				return false, err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", idempotencyKey)
			// Keep retries in this loop, including their delay and metric count.
			req.GetBody = nil

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return true, fmt.Errorf("[POST /payments] %w: %w", erroredUpstream, err)
			}
			defer res.Body.Close()

			// Consume error responses too, allowing the connection to be reused.
			_, _ = io.Copy(io.Discard, res.Body)
			if res.StatusCode == http.StatusNoContent {
				return false, nil
			}
			// 409 means the same key is still processing, never success.
			// Other client errors (including 400 and 422) are permanent.
			retryable := res.StatusCode == http.StatusConflict || res.StatusCode == http.StatusRequestTimeout ||
				res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500
			return retryable, fmt.Errorf("[POST /payments] unexpected status code (%d): %w", res.StatusCode, erroredUpstream)
		}()
		if err != nil {
			if retryable && ctx.Err() == nil && retry < 5 {
				retry++
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				return retry, err
			}
		}
		break
	}

	return retry, nil
}
