package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	Version string
)

// setupPprof registers pprof handlers only on the pprof server
func setupPprof() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	return mux
}

func main() {
	cfgFile := flag.String("config", "", "Path to a config file")
	flag.Parse()

	cfg, err := configLoad(*cfgFile)
	if err != nil {
		log.Fatal(err)
	}

	// Start pprof server with dedicated mux (only if configured)
	if cfg.ListenPprof != "" {
		go func() {
			pprofMux := setupPprof()
			server := &http.Server{
				Addr:         cfg.ListenPprof,
				Handler:      pprofMux, // Use dedicated mux
				ReadTimeout:  cfg.Timeout,
				WriteTimeout: cfg.Timeout,
				IdleTimeout:  cfg.IdleTimeout,
			}
			log.Infof("Starting pprof server on %s", cfg.ListenPprof)
			if err := server.ListenAndServe(); err != nil {
				log.Fatalf("Unable to listen on %s: %s", cfg.ListenPprof, err)
			}
		}()
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		server := &http.Server{
			Addr:         cfg.ListenMetricsAddress,
			Handler:      nil,
			ReadTimeout:  cfg.Timeout,
			WriteTimeout: cfg.Timeout,
			IdleTimeout:  cfg.IdleTimeout,
		}
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Unable to listen on %s: %s", cfg.ListenMetricsAddress, err)
		}
	}()

	lvl, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Fatalf("Unable to parse log level: %s", err)
	}

	log.SetLevel(lvl)

	cfgJSON, _ := json.Marshal(cfg)
	log.Warnf("Effective config: %+v", string(cfgJSON))

	proc := newProcessor(*cfg)

	if err = proc.run(); err != nil {
		log.Fatalf("Unable to start: %s", err)
	}

	log.Warnf("Listening on %s, sending to %s", cfg.Listen, cfg.Target)
	log.Warnf("Started %s", Version)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	<-ch

	log.Warn("Shutting down, draining requests")
	if err = proc.close(); err != nil {
		log.Errorf("Error during shutdown: %s", err)
	}

	log.Warnf("Finished")
}
