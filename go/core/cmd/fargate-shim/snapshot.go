package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const maxSnapshotBytes = 512 << 20

// snapshotRef tells a pod where to store or restore its own /data. The token only
// opens the object of one Actor UID.
type snapshotRef struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

type snapshots struct {
	baseURL string
	secret  []byte
}

func (s *snapshots) enabled() bool { return s != nil && s.baseURL != "" && len(s.secret) > 0 }

func tokenFor(secret []byte, subject string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(subject))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *snapshots) refFor(uid string) *snapshotRef {
	if !s.enabled() || uid == "" {
		return nil
	}
	return &snapshotRef{URL: strings.TrimRight(s.baseURL, "/") + "/v1/data/" + uid, Token: tokenFor(s.secret, uid)}
}

func (s *snapshots) delete(ctx context.Context, uid string) error {
	ref := s.refFor(uid)
	if ref == nil {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, ref.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Ate-Admin", tokenFor(s.secret, "admin"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete snapshot: %s", resp.Status)
	}
	return nil
}

// storeHandler keeps one gzip tar per Actor UID on a local volume.
func storeHandler(dir string, secret []byte) http.Handler {
	var mu sync.Mutex
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid, ok := strings.CutPrefix(r.URL.Path, "/v1/data/")
		if _, err := uuid.Parse(uid); !ok || err != nil {
			http.Error(w, "unknown path", http.StatusNotFound)
			return
		}
		admin := hmac.Equal([]byte(r.Header.Get("X-Ate-Admin")), []byte(tokenFor(secret, "admin")))
		bearer, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		own := hmac.Equal([]byte(bearer), []byte(tokenFor(secret, uid)))
		file := filepath.Join(dir, uid+".tar.gz")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodDelete && admin:
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && (own || admin):
			f, err := os.Open(file)
			if os.IsNotExist(err) {
				http.Error(w, "no snapshot", http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer f.Close()
			_, _ = io.Copy(w, f)
		case r.Method == http.MethodPut && own:
			tmp, err := os.CreateTemp(dir, uid+".*.tmp")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer os.Remove(tmp.Name())
			if _, err := io.Copy(tmp, http.MaxBytesReader(w, r.Body, maxSnapshotBytes)); err != nil {
				tmp.Close()
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			if err := tmp.Close(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if err := os.Rename(tmp.Name(), file); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "forbidden", http.StatusForbidden)
		}
	})
}

func tarDir(w io.Writer, dir string) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// untar refuses absolute paths, parent references and every entry that is not a
// plain file or directory, so a snapshot cannot write outside the target.
func untar(r io.Reader, dest string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := filepath.Clean(filepath.FromSlash(hdr.Name))
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			return fmt.Errorf("unsafe path %q in snapshot", hdr.Name)
		}
		target := filepath.Join(dest, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o777); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o777); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, io.LimitReader(tr, maxSnapshotBytes)); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported entry %q in snapshot", hdr.Name)
		}
	}
}

// restoreData downloads the Actor's snapshot into dest. A missing snapshot is a first start.
func restoreData(ctx context.Context, ref snapshotRef, dest string) error {
	if ref.URL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+ref.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil
	case http.StatusOK:
		return untar(resp.Body, dest)
	}
	return fmt.Errorf("restore snapshot: %s", resp.Status)
}

func saveData(ctx context.Context, ref snapshotRef, src string) error {
	var body bytes.Buffer
	if err := tarDir(&body, src); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, ref.URL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+ref.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("save snapshot: %s", resp.Status)
	}
	return nil
}

// sidecarHandler saves /data on request; the shim calls it when an Actor suspends.
func sidecarHandler(src string) http.Handler {
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/save", func(w http.ResponseWriter, r *http.Request) {
		var ref snapshotRef
		if r.Method != http.MethodPost || json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&ref) != nil || ref.URL == "" {
			http.Error(w, "POST {url, token} expected", http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		if err := saveData(ctx, ref, src); err != nil {
			slog.Warn("save snapshot", "error", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// requestSave asks the pod's snapshot sidecar to upload /data.
func requestSave(ctx context.Context, podIP string, ref snapshotRef) error {
	raw, _ := json.Marshal(ref)
	ctx, cancel := context.WithTimeout(ctx, 100*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+podIP+":9090/save", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("sidecar save: %s", resp.Status)
	}
	return nil
}
