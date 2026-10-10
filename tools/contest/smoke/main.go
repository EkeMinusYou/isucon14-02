// Command smoke checks a bounded ISURIDE session flow without a benchmark.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"time"
)

func main() {
	base := flag.String("base", "https://isuride.xiv.isucon.net", "application base URL")
	flag.Parse()
	app, owner, chair := newClient(*base), newClient(*base), newClient(*base)
	name := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
	app.do("GET", "/", nil, http.StatusOK, nil)
	app.do("POST", "/api/app/users", map[string]string{
		"username": name, "firstname": "Setup", "lastname": "Smoke", "date_of_birth": "2000-01-01",
	}, http.StatusCreated, nil)
	app.requireCookie("app_session")
	var registeredOwner struct {
		ChairRegisterToken string `json:"chair_register_token"`
	}
	owner.do("POST", "/api/owner/owners", map[string]string{"name": name}, http.StatusCreated, &registeredOwner)
	owner.requireCookie("owner_session")
	if registeredOwner.ChairRegisterToken == "" {
		log.Fatal("owner registration returned no chair register token")
	}
	chair.do("POST", "/api/chair/chairs", map[string]string{
		"name": name, "model": "リラックスシート NEO", "chair_register_token": registeredOwner.ChairRegisterToken,
	}, http.StatusCreated, nil)
	chair.requireCookie("chair_session")
	chair.do("POST", "/api/chair/activity", map[string]bool{"is_active": false}, http.StatusNoContent, nil)
	chair.do("POST", "/api/chair/coordinate", map[string]int{"latitude": 0, "longitude": 0}, http.StatusOK, nil)
	for i := 0; i < 4; i++ {
		app.do("GET", "/api/app/rides", nil, http.StatusOK, nil)
		app.do("GET", "/api/app/nearby-chairs?latitude=0&longitude=0&distance=1", nil, http.StatusOK, nil)
		owner.do("GET", "/api/owner/chairs", nil, http.StatusOK, nil)
		owner.do("GET", "/api/owner/sales", nil, http.StatusOK, nil)
		time.Sleep(250 * time.Millisecond)
	}
	fmt.Println("Smoke: app, owner and inactive chair sessions, authenticated reads and JSON responses passed")
}

type client struct {
	http *http.Client
	base string
}

func newClient(base string) *client {
	jar, err := cookiejar.New(nil)
	if err != nil {
		log.Fatal(err)
	}
	return &client{http: &http.Client{Jar: jar, Timeout: 15 * time.Second}, base: strings.TrimSuffix(base, "/")}
}

func (c *client) do(method, path string, body any, wantStatus int, dest any) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		log.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		log.Fatalf("%s %s: transport failed: %v", method, path, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != wantStatus {
		log.Fatalf("%s %s: status %d, expected %d, read error %v", method, path, res.StatusCode, wantStatus, err)
	}
	if res.Header.Get("X-Measurement-Session-ID") != "" {
		log.Fatal("measurement session header was exposed to the client")
	}
	if strings.HasPrefix(path, "/api/") && wantStatus != http.StatusNoContent && !json.Valid(data) {
		log.Fatalf("%s %s: invalid JSON", method, path)
	}
	if dest != nil {
		if err := json.Unmarshal(data, dest); err != nil {
			log.Fatal("invalid registration response")
		}
	}
}

func (c *client) requireCookie(name string) {
	req, _ := http.NewRequest("GET", c.base, nil)
	for _, cookie := range c.http.Jar.Cookies(req.URL) {
		if cookie.Name == name && cookie.Value != "" {
			return
		}
	}
	log.Fatalf("registration did not set %s", name)
}
