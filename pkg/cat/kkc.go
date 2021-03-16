package cat

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kentik/ktranslate/pkg/cat/api"
	"github.com/kentik/ktranslate/pkg/cat/auth"
	"github.com/kentik/ktranslate/pkg/filter"
	"github.com/kentik/ktranslate/pkg/formats"
	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/kentik/ktranslate/pkg/rollup"
	ss "github.com/kentik/ktranslate/pkg/sinks"
	"github.com/kentik/ktranslate/pkg/sinks/kentik"
	"github.com/kentik/ktranslate/pkg/snmp"
	"github.com/kentik/ktranslate/pkg/util/gopatricia/patricia"
	model "github.com/kentik/ktranslate/pkg/util/kflow2"

	"github.com/kentik/ktranslate/pkg/eggs/kmux"

	"github.com/kentik/ktranslate/pkg/eggs/baseserver"
	"github.com/kentik/ktranslate/pkg/eggs/logger"

	go_metrics "github.com/kentik/go-metrics"
	old_logger "github.com/kentik/golog/logger"

	"github.com/bmatsuo/lmdb-go/lmdb"
	capn "zombiezen.com/go/capnproto2"
)

// Setting this decode limit explicitly so that we know what it is.
// By default at the time of this writing the library would have used 64MiB.
// Feel free to change if appropriate.
const (
	kentikDefaultCapnprotoDecodeLimit = 128 << 20 // 128 MiB
	CHAN_SLACK                        = 8000      // Up to this many messages / sec
	MetricsCheckDuration              = 60 * time.Second
	CacheInvalidateDuration           = 8 * time.Hour
	SendBatchDuration                 = 1 * time.Second
	MDB_NO_LOCK                       = 0x400000
	MDB_PERMS                         = 0666
)

var (
	RollupsSendDuration = 15 * time.Second
)

func NewKTranslate(config *Config, log logger.ContextL, registry go_metrics.Registry, version string, ol *old_logger.Logger, sinks string) (*KTranslate, error) {
	kc := &KTranslate{
		log:      log,
		registry: registry,
		config:   config,
		metrics: &KKCMetric{
			Flows:        go_metrics.GetOrRegisterMeter("flows", registry),
			FlowsOut:     go_metrics.GetOrRegisterMeter("flows_out", registry),
			DroppedFlows: go_metrics.GetOrRegisterMeter("dropped_flows", registry),
			Errors:       go_metrics.GetOrRegisterMeter("errors", registry),
			AlphaQ:       go_metrics.GetOrRegisterGauge("alphaq", registry),
			AlphaQDrop:   go_metrics.GetOrRegisterMeter("alphaq_drop", registry),
			JCHFQ:        go_metrics.GetOrRegisterGauge("jchfq", registry),
			Snmp:         go_metrics.GetOrRegisterMeter("snmp", registry),
		},
		alphaChans: make([]chan *Flow, config.Threads),
		jchfChans:  make([]chan *kt.JCHF, config.Threads),
		msgsc:      make(chan []byte, 60),
		ol:         ol,
	}

	for i := 0; i < config.Threads; i++ {
		kc.jchfChans[i] = make(chan *kt.JCHF, CHAN_SLACK)
		for j := 0; j < CHAN_SLACK; j++ {
			kc.jchfChans[i] <- kt.NewJCHF()
		}
	}

	log.Infof("Turning on %d processing threads", config.Threads)
	for i := 0; i < config.Threads; i++ {
		kc.alphaChans[i] = make(chan *Flow, CHAN_SLACK)
	}

	// Load any rollups we are doing
	rolls, err := rollup.GetRollups(log.GetLogger().GetUnderlyingLogger())
	if err != nil {
		return nil, err
	}
	kc.rollups = rolls
	kc.doRollups = len(rolls) > 0

	// And load any filters we are doing
	filters, err := filter.GetFilters(log.GetLogger().GetUnderlyingLogger())
	if err != nil {
		return nil, err
	}
	kc.filters = filters

	// Load up our region and city mappers.
	if config.Code2City != "" {
		envCity, _ := lmdb.NewEnv()
		if err := envCity.Open(config.Code2City, MDB_NO_LOCK, MDB_PERMS); err == nil {
			kc.envCode2City = envCity
			log.Infof("Loaded Code2city from %s", config.Code2City)
		} else {
			log.Errorf("Cannot open Code2City from %s", config.Code2Region)
			envCity.Close()
			return nil, err
		}
	}

	if config.Code2Region != "" {
		envRegion, _ := lmdb.NewEnv()
		if err := envRegion.Open(config.Code2Region, MDB_NO_LOCK, MDB_PERMS); err == nil {
			kc.envCode2Region = envRegion
			log.Infof("Loaded Code2Region from %s", config.Code2Region)
		} else {
			log.Infof("Cannot open Code2Region from %s", config.Code2Region)
			envRegion.Close()
			return nil, err
		}
	}

	// Grab the custom data directly from a file.
	if config.MappingFile != "" {
		m, err := NewCustomMapper(config.MappingFile)
		if err != nil {
			return nil, err
		}
		kc.mapr = m
		kc.log.Infof("Loaded %d custom mappings", len(m.Customs))
	}

	if config.UDRFile != "" {
		m, udrs, err := NewUDRMapper(config.UDRFile, config.Subtype)
		if err != nil {
			return nil, err
		}
		kc.udrMapr = m
		if m.Subtype == nil {
			kc.log.Infof("Loaded %d udr mappings with %d udrs", len(m.UDRs), udrs)
		} else {
			kc.log.Infof("Loaded %d udr mappings for subtype %s", len(m.Subtype), config.Subtype)
		}
	}

	// Load up a geo file if one is passed in.
	if config.GeoMapping != "" {
		geo, err := patricia.OpenGeo(config.GeoMapping, false, ol)
		if err != nil {
			return nil, err
		} else {
			kc.geo = geo
		}
	}

	// Load asn mapper if set.
	if config.Asn4 != "" && config.Asn6 != "" {
		asn, err := patricia.OpenASN(config.Asn4, config.Asn6, ol)
		if err != nil {
			return nil, err
		} else {
			kc.asn = asn
		}
	}

	kc.log.Infof("Disabling tagging service")

	// Define our sinks for where to send data to.
	kc.sinks = make(map[ss.Sink]ss.SinkImpl)
	for _, sinkStr := range strings.Split(sinks, ",") {
		sink := ss.Sink(sinkStr)
		snk, err := ss.NewSink(sink, log.GetLogger().GetUnderlyingLogger(), registry)
		if err != nil {
			return nil, fmt.Errorf("Invalid sink: %s, %v", sink, err)
		}
		kc.sinks[sink] = snk
		kc.log.Infof("Using sink %s", sink)

		// Kentik gets special cased
		if sink == ss.KentikSink {
			kc.kentik = snk.(*kentik.KentikSink)
		}
	}

	if len(kc.sinks) == 0 {
		return nil, fmt.Errorf("No sinks set")
	}

	// Get some randomness
	rand.Seed(time.Now().UnixNano())

	return kc, nil
}

// nolint: errcheck
func (kc *KTranslate) cleanup() {
	snmp.Close()
	for _, sink := range kc.sinks {
		sink.Close()
	}
	if kc.pgdb != nil {
		kc.pgdb.Close()
	}
	if kc.envCode2Region != nil {
		kc.envCode2Region.Close()
	}
	if kc.envCode2City != nil {
		kc.envCode2City.Close()
	}
	if kc.geo != nil {
		kc.geo.Close()
	}
	if kc.asn != nil {
		kc.asn.Close()
	}
}

// GetStatus implements the baseserver.Service interface.
func (kc *KTranslate) GetStatus() []byte {
	return []byte("OK")
}

// RunHealthCheck implements the baseserver.Service interface.
func (kc *KTranslate) RunHealthCheck(ctx context.Context, result *baseserver.HealthCheckResult) {
}

// HttpInfo implements the baseserver.Service interface.
func (kc *KTranslate) HttpInfo(w http.ResponseWriter, r *http.Request) {
	total := 0
	for _, c := range kc.alphaChans {
		total += len(c)
	}
	kc.metrics.AlphaQ.Update(int64(total)) // Update these on demand.

	total = 0
	for _, c := range kc.jchfChans {
		total += len(c)
	}
	kc.metrics.JCHFQ.Update(int64(total))
	h := hc{
		Flows:          kc.metrics.Flows.Rate1(),
		FlowsOut:       kc.metrics.FlowsOut.Rate1(),
		DroppedFlows:   kc.metrics.DroppedFlows.Rate1(),
		Errors:         kc.metrics.Errors.Rate1(),
		AlphaQ:         kc.metrics.AlphaQ.Value(),
		JCHFQ:          kc.metrics.JCHFQ.Value(),
		AlphaQDrop:     kc.metrics.AlphaQDrop.Rate1(),
		Snmp:           kc.metrics.Snmp.Rate1(),
		Sinks:          map[ss.Sink]map[string]float64{},
		SnmpDeviceData: map[string]map[string]float64{},
	}

	// Now, let other sinks do their work
	for sn, sink := range kc.sinks {
		h.Sinks[sn] = sink.HttpInfo()
	}

	if kc.metrics.SnmpDeviceData != nil {
		kc.metrics.SnmpDeviceData.Mux.RLock()
		for d, met := range kc.metrics.SnmpDeviceData.Devices {
			h.SnmpDeviceData[d] = map[string]float64{
				"DeviceMetrics":    met.DeviceMetrics.Rate1(),
				"InterfaceMetrics": met.InterfaceMetrics.Rate1(),
				"Metadata":         met.Metadata.Rate1(),
				"Errors":           met.Errors.Rate1(),
			}
		}
	}

	b, err := json.Marshal(h)
	if err != nil {
		kc.log.Errorf("Error in HC: %v", err)
	} else {
		w.Write(b)
	}
}

// Handler for json data, useful for testing mostly. Requires you to set content-type: application/json
func (kc *KTranslate) handleJson(cid kt.Cid, raw []byte) error {
	serBuf := make([]byte, 0)
	select {
	case jflow := <-kc.jchfChans[0]: // non blocking select on this chan.
		err := json.Unmarshal(raw, jflow)
		if err != nil {
			return err
		} else {
			res, err := kc.format.To([]*kt.JCHF{jflow}, serBuf)
			jflow.Reset()
			kc.jchfChans[0] <- jflow
			if err != nil {
				return err
			}
			kc.msgsc <- res // Send  on without batching.
		}
	default: // We're out of batched flows, just drop this one.
		kc.metrics.DroppedFlows.Mark(1)
	}
	return nil
}

// Take flow from http requests, deserialize and pass it on to alphaChan
// Gets called from a goroutine-per-request
func (kc *KTranslate) handleFlow(w http.ResponseWriter, r *http.Request) {
	var err error

	defer func() {
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			kc.metrics.Errors.Mark(1)
			kc.log.Errorf("Error handling request: %v", err)
			fmt.Fprint(w, "BAD") // nolint: errcheck
		} else {
			fmt.Fprint(w, "GOOD") // nolint: errcheck
		}
	}()

	// Decode body in gzip format if the request header is set this way.
	body := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		z, err := gzip.NewReader(r.Body)
		if err != nil {
			kc.log.Errorf("Decompressing body: %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		body = z
	}
	defer body.Close()

	// check company id and other values.
	vals := r.URL.Query()
	senderId := vals.Get(HttpSenderID)
	cidBase := vals.Get(HttpCompanyID)
	cid, err := strconv.Atoi(cidBase)
	if err != nil {
		return
	}

	// Allocate a buffer for the expected size of the incoming data.
	var bodyBufferBytes []byte
	contentLengthString := r.Header.Get("Content-Length")
	if contentLengthString != "" {
		size, err := strconv.Atoi(contentLengthString)
		if err != nil {
			kc.log.Errorf("Reading content length: %+v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if size > 0 &&
			size < MaxProxyListenerBufferAlloc { // limit in case attacker sets Content-Length
			// superstitiously add extra breathing room to buffer just in case
			bodyBufferBytes = make([]byte, 0, size+(2*bytes.MinRead))
		}
	}

	// Read all data from the request (possibly gzip decoding, possibly not)
	bodyBuffer := bytes.NewBuffer(bodyBufferBytes)
	_, err = bodyBuffer.ReadFrom(body)

	if err != nil {
		kc.log.Errorf("Reading body: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	evt := bodyBuffer.Bytes()

	// If its http/json data, treat sperately
	if r.Header.Get("Content-Type") == "application/json" {
		err = kc.handleJson(kt.Cid(cid), evt)
		return
	}

	// If we are sending from before kentik, add offset in here.
	offset := 0
	if senderId != "" && len(evt) > MSG_KEY_PREFIX && // Direct flow without enrichment.
		(evt[0] == 0x00 && evt[1] == 0x00 && evt[2] == 0x00 && evt[3] == 0x00 && evt[4] == 0x00) { // Double check with this
		offset = MSG_KEY_PREFIX

		// If we have a kentik sink, send on here. Don't let flow from kentik get looped back into kentik.
		if kc.kentik != nil {
			go kc.kentik.SendKentik(evt, cid, senderId)
		}
	}

	// decompress and read (capnproto "packed" representation)
	decoder := capn.NewPackedDecoder(bytes.NewBuffer(evt[offset:]))
	decoder.MaxMessageSize = kentikDefaultCapnprotoDecodeLimit
	capnprotoMessage, err := decoder.Decode()
	if err != nil {
		return
	}

	// unpack flow messages and pass them down
	packedCHF, err := model.ReadRootPackedCHF(capnprotoMessage)
	if err != nil {
		return
	}

	messages, err := packedCHF.Msgs()
	if err != nil {
		return
	}

	var sent, dropped int64
	next := 0
	for i := 0; i < messages.Len(); i++ {
		msg := messages.At(i)
		if !msg.Big() { // Don't work on low res data
			if !msg.SampleAdj() {
				msg.SetSampleRate(msg.SampleRate() * 100) // Apply re-sample trick here.
			}

			// send without blocking, dropping the message if the channel buffer is full
			alpha := &Flow{CompanyId: cid, CHF: msg}
			select {
			case kc.alphaChans[next] <- alpha:
				sent++
			default:
				dropped++
			}
			next++ // Round robin across processing threads.
			if next >= kc.config.Threads {
				next = 0
			}
		}
	}
	kc.metrics.Flows.Mark(sent)
	kc.metrics.DroppedFlows.Mark(dropped)
}

func (kc *KTranslate) doSend(ctx context.Context) {
	kc.log.Infof("do sendToKTranslate Starting")

	for {
		select {
		case ser := <-kc.msgsc:
			if len(ser) == 0 {
				continue
			}

			for _, sink := range kc.sinks {
				sink.Send(ctx, ser)
			}

		case <-ctx.Done():
			kc.log.Infof("do sendToKTranslate Done")
			return
		}
	}
}

func (kc *KTranslate) sendToSinks(ctx context.Context) error {

	metricsTicker := time.NewTicker(MetricsCheckDuration)
	defer metricsTicker.Stop()

	rollupsTicker := time.NewTicker(RollupsSendDuration)
	defer rollupsTicker.Stop()

	// This one is in charge of sending on to sinks.
	go kc.doSend(ctx)
	kc.log.Infof("sendToSinks base Online")

	// These do the actual processing now.
	for i := 0; i < kc.config.Threads; i++ {
		go kc.monitorAlphaChan(ctx, i, kc.format.To)
	}

	for {
		select {
		case <-metricsTicker.C:
			total := 0
			for _, c := range kc.alphaChans {
				total += len(c)
			}
			kc.metrics.AlphaQ.Update(int64(total))

			total = 0
			for _, c := range kc.jchfChans {
				total += len(c)
			}
			kc.metrics.JCHFQ.Update(int64(total))

		case <-rollupsTicker.C:
			for _, r := range kc.rollups {
				export := r.Export()
				if len(export) > 0 {
					res, err := kc.format.Rollup(export)
					if err != nil {
						kc.log.Errorf("Cannot handle rollup: %v", err)
					} else {
						kc.msgsc <- res
					}
				}
			}

		case <-ctx.Done():
			kc.log.Infof("sendToSinks base Done")
			return nil
		}
	}
}

func (kc *KTranslate) monitorSnmp(ctx context.Context, seri func([]*kt.JCHF, []byte) ([]byte, error)) {
	kc.log.Infof("monitorSnmp Starting")
	serBuf := make([]byte, 0)
	for {
		select {
		case msgs := <-kc.snmpChan:
			// If we have any rollups defined, send here instead of directly to the output format.
			if kc.doRollups {
				for _, r := range kc.rollups {
					r.Add(msgs)
				}
			}

			// Turn into a binary format here, using the passed in encoder.
			if !kc.doRollups || kc.config.RollupAndAlpha {
				ser, err := seri(msgs, serBuf)
				if err != nil {
					kc.log.Errorf("Converting to native: %v", err)
				} else {
					kc.msgsc <- ser
				}
			}

			kc.metrics.Snmp.Mark(int64(len(msgs)))
		case <-ctx.Done():
			kc.log.Infof("monitorSnmp Done")
			return
		}
	}
}

func (kc *KTranslate) monitorAlphaChan(ctx context.Context, i int, seri func([]*kt.JCHF, []byte) ([]byte, error)) {
	cacheTicker := time.NewTicker(CacheInvalidateDuration)
	defer cacheTicker.Stop()

	sendTicker := time.NewTicker(SendBatchDuration)
	defer sendTicker.Stop()

	// Set up some data structures.
	company := make(map[kt.Cid]kt.Devices)
	if kc.apic != nil {
		c, err := kc.apic.GetDevices(ctx)
		if err != nil {
			kc.log.Errorf("Cannot get devices: %v", err)
		} else {
			company = c
		}
	}
	citycache := map[uint32]string{}
	regioncache := map[uint32]string{}
	tagcache := map[uint64]string{}
	serBuf := make([]byte, 0)
	msgs := make([]*kt.JCHF, 0)
	sendBytesOn := func() {
		if len(msgs) == 0 {
			return
		}

		// If we have any rollups defined, send here instead of directly to the output format.
		if kc.doRollups {
			for _, r := range kc.rollups {
				r.Add(msgs)
			}
		}

		// Turn into a binary format here, using the passed in encoder.
		if !kc.doRollups || kc.config.RollupAndAlpha {
			// Compute and sample rate stuff here.
			keep := len(msgs)
			if kc.config.SampleRate > 1 {
				rand.Shuffle(len(msgs), func(i, j int) {
					msgs[i], msgs[j] = msgs[j], msgs[i]
				})
				keep = int(math.Max(float64(len(msgs))/float64(kc.config.SampleRate), 1))
				for _, msg := range msgs {
					msg.SampleRate = msg.SampleRate * kc.config.SampleRate
				}
			}
			ser, err := seri(msgs[0:keep], serBuf)
			if err != nil {
				kc.log.Errorf("Converting to native: %v", err)
			} else {
				kc.msgsc <- ser
			}
		}

		for _, m := range msgs { // Give back our cache.
			m.Reset()
			kc.jchfChans[i] <- m
		}

		// match in with out.
		kc.metrics.FlowsOut.Mark(int64(len(msgs)))
		msgs = make([]*kt.JCHF, 0)
	}

	currentTime := time.Now().Unix() // Record rough time of flow sent.
	kc.log.Infof("sendToSink %d Online", i)
	for {
		select {
		case f := <-kc.alphaChans[i]:
			select {
			case jflow := <-kc.jchfChans[i]: // non blocking select on this chan.
				err := kc.flowToJCHF(ctx, company, citycache, regioncache, jflow, f, currentTime, tagcache)
				if err != nil {
					kc.log.Errorf("Cannot convert to json: %v", err)
					jflow.Reset()
					kc.jchfChans[i] <- jflow
					continue
				}
				keep := true
				for _, f := range kc.filters {
					if !f.Filter(jflow) {
						keep = false
						break
					}
				}
				if keep {
					msgs = append(msgs, jflow) // Batch up here.
					if len(msgs) >= kc.config.MaxFlowPerMessage {
						sendBytesOn()
					}
				} else {
					kc.jchfChans[i] <- jflow // Toss this guy, he doesn't meet out filter.
				}

			default: // We're out of batched flows, send what we have and re-q this one.
				sendBytesOn()
				select {
				case kc.alphaChans[i] <- f:
				default:
					kc.metrics.DroppedFlows.Mark(1)
				}
			}
		case _ = <-sendTicker.C: // Send on here.
			sendBytesOn() // Has context for everything it needs.
			currentTime = time.Now().Unix()

		case <-cacheTicker.C:
			company = make(map[kt.Cid]kt.Devices)
			tagcache = map[uint64]string{}
			if kc.apic != nil {
				c, err := kc.apic.GetDevices(ctx)
				if err != nil {
					kc.log.Errorf("Cannot get devices: %v", err)
				} else {
					company = c
				}
			}

		case <-ctx.Done():
			kc.log.Infof("sendToSink %d Done", i)
			return
		}
	}
}

func (kc *KTranslate) getRouter() http.Handler {
	r := kmux.NewRouter()
	r.HandleFunc(HttpAlertInboundPath, kc.handleFlow)
	r.HandleFunc(HttpHealthCheckPath, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK\n") // nolint: errcheck
	})
	if kc.auth != nil {
		kc.auth.RegisterRoutes(r)
	}

	return r
}

func (kc *KTranslate) listenHTTP() {
	kc.log.Infof("Setting up HTTP system on %s%s", kc.config.Listen, HttpAlertInboundPath)
	server := &http.Server{Addr: kc.config.Listen, Handler: kc.getRouter()}
	err := server.ListenAndServe()
	// err is always non-nil -- the http server stopped.
	if err != http.ErrServerClosed {
		kc.log.Errorf("While bringing up HTTP system on %s -- %v", kc.config.Listen, err)
		panic(err)
	}
	kc.log.Infof("HTTP server shut down on %s -- %v", kc.config.Listen, err)
}

func (kc *KTranslate) Run(ctx context.Context) error {
	defer kc.cleanup()

	// DNS mapper if set.
	if kc.config.DnsResolver != "" {
		res, err := NewResolver(ctx, kc.log.GetLogger().GetUnderlyingLogger(), kc.config.DnsResolver)
		if err != nil {
			return err
		}
		kc.resolver = res
		kc.log.Infof("Enabled DNS resolution at: %s", kc.config.DnsResolver)
	}

	// Set up formatter
	fmtr, err := formats.NewFormat(kc.config.Format, kc.log.GetLogger().GetUnderlyingLogger(), kc.config.Compression)
	if err != nil {
		return err
	}
	kc.format = fmtr

	// Connect our sinks.
	for _, sink := range kc.sinks {
		err := sink.Init(ctx, kc.config.Format, kc.config.Compression, kc.format)
		if err != nil {
			return err
		}
	}

	// Set up api auth system if this is set. Allows kproxy|kprobe|kappa|ksynth and others to use this without phoneing home to kentik.
	if kc.config.Auth != nil {
		authr, err := auth.NewServer(kc.config.Auth.DevicesFile, kc.log)
		if err != nil {
			return err
		}
		kc.auth = authr
	}

	// Api system for talking to kentik.
	if kc.config.Kentik.ApiEmail != "" {
		apic, err := api.NewKentikApi(ctx, kc.config.Kentik.ApiEmail, kc.config.Kentik.ApiToken, kc.config.Kentik.ApiRoot, kc.log)
		if err != nil {
			return err
		}
		kc.apic = apic
	}

	// If SNMP is configured, start this system too. Poll for metrics and metadata, also handle traps.
	if kc.config.SNMPFile != "" {
		if kc.config.SNMPDisco { // Here, we're just returning the list of devices on the network which might speak snmp.
			return snmp.Discover(ctx, kc.config.SNMPFile, kc.log)
		}
		kc.snmpChan = make(chan []*kt.JCHF, CHAN_SLACK)
		kc.metrics.SnmpDeviceData = kt.NewSnmpMetricSet(kc.registry)
		err := snmp.StartSNMPPolls(kc.config.SNMPFile, kc.snmpChan, kc.metrics.SnmpDeviceData, kc.registry, kc.log)
		if err != nil {
			return err
		}
		go kc.monitorSnmp(ctx, kc.format.To)
	}

	kc.log.Infof("System running with format %s, compression %s, max flows: %d, sample rate %d:1", kc.config.Format, kc.config.Compression, kc.config.MaxFlowPerMessage, kc.config.SampleRate)
	go kc.listenHTTP()
	return kc.sendToSinks(ctx)
}

func (kc *KTranslate) Close() {
	// this service uses the ctx object passed in Run, do nothing here
}
