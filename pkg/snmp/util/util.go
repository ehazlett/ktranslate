package util

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/kentik/ktranslate/pkg/eggs/logger"
	"github.com/kentik/gosnmp"
)

var (
	SNMP_POLL_SLEEP_TIME = 60 * time.Second

	// Device Manufacturer aka sysDescr
	SNMP_DEVICE_MANUFACTURER_OID = "1.3.6.1.2.1.1.1"
)

func ContainsAny(s string, substrs ...string) bool {
	for _, substr := range substrs {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}

var (
	TRUNCATE     = true
	NO_TRUNCATE  = false
	MAX_SNMP_LEN = 128
)

func ReadOctetString(variable gosnmp.SnmpPDU, truncate bool) (string, bool) {
	if variable.Type != gosnmp.OctetString {
		return "", false
	}

	value := variable.Value.([]byte)
	value = bytes.Trim(value, "\x00")
	if truncate && len(value) > MAX_SNMP_LEN {
		value = value[0:MAX_SNMP_LEN]
	}
	return string(value), true
}

// toInt64 converts most of the numeric types in Go (and *all* the numeric
// types I expect to see from snmp) to int64.
func ToInt64(val interface{}) int64 {
	switch v := val.(type) {
	case uint:
		return int64(v)
	case uint8: // same as byte
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		return int64(v)
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32: // same as rune
		return int64(v)
	case int64:
		return int64(v)
	}
	return 0 // Probably not reached, in context.
}

// getIndex returns the rest of value after prefix, e.g. for 1.2.3.4.5.6 and
// 2.3.4, returns .5.6  Modeled on getIndex in
// topology-demo/devicemetrics/main.go.  Prefix must occur in value, otherwise
// results are undefined (aka "wrong").
func GetIndex(value, prefix string) string {
	return value[strings.Index(value, prefix)+len(prefix):]
}

// walk the OID subtree under a root, returning a slice of varbinds
func WalkOID(oid string, server *gosnmp.GoSNMP, log logger.ContextL, logName string) ([]gosnmp.SnmpPDU, error) {

	// New strategy -- for each varbind, we'll try three times:
	//   first, with GetBulk
	//   if that fails, try GetNext without sleeping in-between
	//   if that fails, sleep for a while, then try GetNext again
	// The first retry is really a fallback -- see if the device is just unresponsive
	// to some GetBulk requests.  Turns out that happens reasonably frequently.
	// The second one is to see if there's a temporary load issue on the device,
	// and if we wait a while, things will get better.
	type pollTry struct {
		walk  func(string) ([]gosnmp.SnmpPDU, error)
		sleep time.Duration
	}

	tries := []pollTry{
		pollTry{walk: server.BulkWalkAll, sleep: time.Duration(0)},
		pollTry{walk: server.WalkAll, sleep: time.Duration(0)},
		pollTry{walk: server.WalkAll, sleep: SNMP_POLL_SLEEP_TIME},
	}

	var err error
	var results []gosnmp.SnmpPDU
	for i, try := range tries {
		time.Sleep(try.sleep)

		results, err = try.walk(oid)
		if err == nil {
			if i > 0 {
				log.Infof("%s SNMP retry %d on OID %s succeeded", logName, i, oid)
			}
			return results, nil
		}

		log.Warnf("%s SNMP retry %d, poll error '%v' walking OID %s", logName, i, err, oid)
	}

	log.Warnf("%s SNMP retry on OID %s failed - giving up", logName, oid)
	return nil, err
}

type snmpWalker interface {
	WalkAll(string) ([]gosnmp.SnmpPDU, error)
}

func GetDeviceManufacturer(server snmpWalker, log logger.ContextL) string {
	results, err := server.WalkAll(SNMP_DEVICE_MANUFACTURER_OID)
	if err != nil {
		log.Debugf("Error retrieving SNMP device manufacturer; ignoring it: %v", err)
		return ""
	}
	if len(results) == 0 {
		return ""
	}
	deviceManufacturerEnc, ok := results[0].Value.([]byte)
	// Don't know why it wouldn't be a []byte, but just in case
	if !ok {
		log.Debugf("getDeviceManufacturer: received a non-[]byte: %v", results[0].Value)
		return ""
	}
	deviceManufacturerBytes, err := base64.StdEncoding.DecodeString(string(deviceManufacturerEnc))
	// An error (probably) just means it's not actually base64 encoded; assume plain text.
	if err != nil {
		deviceManufacturerBytes = deviceManufacturerEnc
	}
	deviceManufacturer :=
		strings.TrimSpace(
			strings.Replace(
				strings.Replace(string(deviceManufacturerBytes), "\n", "/", -1),
				"\r", "", -1))
	deviceManufacturerRunes := []rune(deviceManufacturer)
	if len(deviceManufacturerRunes) > 128 {
		deviceManufacturerRunes = deviceManufacturerRunes[:128]
	}
	deviceManufacturer = strconv.QuoteToASCII(string(deviceManufacturerRunes))
	// Strip the leading & trailing quotes that QuoteToASCII adds.
	deviceManufacturer = deviceManufacturer[1 : len(deviceManufacturer)-1]
	return deviceManufacturer
}
