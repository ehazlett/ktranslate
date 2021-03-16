package cat

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/bmatsuo/lmdb-go/lmdb"

	"github.com/kentik/ktranslate/pkg/kt"
	"github.com/kentik/ktranslate/pkg/util/cdn"
	patricia "github.com/kentik/ktranslate/pkg/util/gopatricia/patricia"
	"github.com/kentik/ktranslate/pkg/util/ic"
	model "github.com/kentik/ktranslate/pkg/util/kflow2"
)

var (
	DEFAULT_GEO        = []byte("--")
	DEFAULT_GEO_PACKED = patricia.PackGeo(DEFAULT_GEO)
)

func (kc *KTranslate) lookupGeo(ipv4 uint32, ipv6 []byte) (*patricia.NodeGeo, error) {
	if ipv4 != 0 {
		return kc.geo.SearchBestFromHostGeo(net.IPv4(byte(ipv4>>24), byte(ipv4>>16), byte(ipv4>>8), byte(ipv4)))
	}
	return kc.geo.SearchBestFromHostGeo(net.IP(ipv6))
}

func (kc *KTranslate) setGeoAsn(src *Flow) {
	// Fetch our own geo if not already set.
	if kc.geo != nil {
		if src.CHF.SrcGeo() == 0 || src.CHF.SrcGeo() == DEFAULT_GEO_PACKED {
			ipv6, _ := src.CHF.Ipv6SrcAddr()
			if srcGeo, err := kc.lookupGeo(src.CHF.Ipv4SrcAddr(), ipv6); err == nil {
				src.CHF.SetSrcGeo(patricia.GetCountry(srcGeo))
				src.CHF.SetSrcGeoRegion(patricia.GetRegion(srcGeo))
				src.CHF.SetSrcGeoCity(patricia.GetCity(srcGeo))
			}
		}

		if src.CHF.DstGeo() == 0 || src.CHF.DstGeo() == DEFAULT_GEO_PACKED {
			ipv6, _ := src.CHF.Ipv6DstAddr()
			if dstGeo, err := kc.lookupGeo(src.CHF.Ipv4DstAddr(), ipv6); err == nil {
				src.CHF.SetDstGeo(patricia.GetCountry(dstGeo))
				src.CHF.SetDstGeoRegion(patricia.GetRegion(dstGeo))
				src.CHF.SetDstGeoCity(patricia.GetCity(dstGeo))
			}
		}
	}

	// And set our own asn also if not set.
	if kc.asn != nil {
		if src.CHF.SrcAs() == 0 {
			ipv6, _ := src.CHF.Ipv6SrcAddr()
			if resultsFound, asn, err := kc.asn.FindBestMatch(src.CHF.Ipv4SrcAddr(), ipv6); resultsFound && err == nil {
				src.CHF.SetSrcAs(asn)
			}
		}

		if src.CHF.DstAs() == 0 {
			ipv6, _ := src.CHF.Ipv6DstAddr()
			if resultsFound, asn, err := kc.asn.FindBestMatch(src.CHF.Ipv4DstAddr(), ipv6); resultsFound && err == nil {
				src.CHF.SetDstAs(asn)
			}
		}
	}
}

func (kc *KTranslate) getEventType(dst *kt.JCHF) string {

	// if app_proto is 12, this is snmp and return as such.
	if dst.CustomInt["APP_PROTOCOL"] == 12 {
		return kt.KENTIK_EVENT_SNMP
	}

	// Else, if its synth, split out into traceroute and other.
	if dst.CustomInt["APP_PROTOCOL"] == 10 {
		if dst.CustomInt["Result Type"] == 4 {
			return kt.KENTIK_EVENT_TRACE
		} else {
			return kt.KENTIK_EVENT_SYNTH
		}
	}

	return kt.KENTIK_EVENT_TYPE
}

func (kc *KTranslate) getProviderType(dst *kt.JCHF) kt.Provider {

	udr, ok := dst.CustomStr[UDR_TYPE]
	if !ok { // Return this right away.
		return kt.ProviderRouter
	}

	// Or maybe its a host.
	if udr == "kprobe" || udr == "kappa" {
		return kt.ProviderHost
	}

	// Else, if its synth, return this.
	if dst.CustomInt["APP_PROTOCOL"] == 10 || dst.CustomInt["APP_PROTOCOL"] == 11 {
		return kt.ProviderSynth
	}

	// Cloud subnet here.
	if strings.HasSuffix(udr, "_subnet") {
		return kt.ProviderVPC
	}

	// Default to router.
	return kt.ProviderRouter
}

func (kc *KTranslate) flowToJCHF(ctx context.Context, company map[kt.Cid]kt.Devices, citycache map[uint32]string, regioncache map[uint32]string, dst *kt.JCHF, src *Flow, currentTS int64, tagcache map[uint64]string) error {

	// In the direct case, users can map their own asn/geo values into here.
	if kc.geo != nil || kc.asn != nil {
		kc.setGeoAsn(src)
	}

	// dst.Timestamp = src.CHF.Timestamp() This is being strage, use current timestamp for now.
	dst.Timestamp = currentTS
	dst.DstAs = src.CHF.DstAs()
	if src.CHF.DstGeo() > 0 {
		dst.DstGeo = fmt.Sprintf("%c%c", src.CHF.DstGeo()>>8, src.CHF.DstGeo()&0xFF)
		if dst.DstGeo[0] == '-' {
			dst.DstGeo = "--"
		}
	}
	dst.HeaderLen = src.CHF.HeaderLen()
	dst.InBytes = src.CHF.InBytes()
	dst.InPkts = src.CHF.InPkts()
	dst.InputPort = kt.IfaceID(src.CHF.InputPort())
	dst.IpSize = src.CHF.IpSize()
	dst.L4DstPort = src.CHF.L4DstPort()
	dst.L4SrcPort = src.CHF.L4SrcPort()
	dst.OutputPort = kt.IfaceID(src.CHF.OutputPort())
	dst.Protocol = src.CHF.Protocol()
	dst.SampledPacketSize = src.CHF.SampledPacketSize()
	dst.SrcAs = src.CHF.SrcAs()
	if src.CHF.SrcGeo() > 0 {
		dst.SrcGeo = fmt.Sprintf("%c%c", src.CHF.SrcGeo()>>8, src.CHF.SrcGeo()&0xFF)
		if dst.SrcGeo[0] == '-' {
			dst.SrcGeo = "--"
		}
	}
	dst.TcpFlags = src.CHF.TcpFlags()
	dst.Tos = src.CHF.Tos()
	dst.VlanIn = src.CHF.VlanIn()
	dst.VlanOut = src.CHF.VlanOut()
	dst.MplsType = src.CHF.MplsType()
	dst.OutBytes = src.CHF.OutBytes()
	dst.OutPkts = src.CHF.OutPkts()
	dst.TcpRetransmit = src.CHF.TcpRetransmit()
	dst.SampleRate = src.CHF.SampleRate() / 100 // Reduce by 100 to get actual rate.
	dst.DeviceId = kt.DeviceID(src.CHF.DeviceId())
	dst.CompanyId = kt.Cid(src.CompanyId)
	dst.SrcNextHopAs = src.CHF.SrcNextHopAs()
	dst.DstNextHopAs = src.CHF.DstNextHopAs()
	dst.SrcGeoRegion = lookupRegionName(regioncache, src.CHF.SrcGeoRegion(), kc.envCode2Region)
	dst.DstGeoRegion = lookupRegionName(regioncache, src.CHF.DstGeoRegion(), kc.envCode2Region)
	dst.SrcGeoCity = lookupCityName(citycache, src.CHF.SrcGeoCity(), kc.envCode2City)
	dst.DstGeoCity = lookupCityName(citycache, src.CHF.DstGeoCity(), kc.envCode2City)
	dst.SrcRoutePrefix = src.CHF.SrcRoutePrefix()
	dst.DstRoutePrefix = src.CHF.DstRoutePrefix()
	dst.SrcSecondAsn = src.CHF.SrcSecondAsn()
	dst.DstSecondAsn = src.CHF.DstSecondAsn()
	dst.SrcThirdAsn = src.CHF.SrcThirdAsn()
	dst.DstThirdAsn = src.CHF.DstThirdAsn()
	dst.CustomStr = make(map[string]string)
	dst.CustomInt = make(map[string]int32)
	dst.CustomBigInt = make(map[string]int64)

	// Do we have info about this device?
	if _, ok := company[dst.CompanyId]; ok {
		if d, ok := company[dst.CompanyId][dst.DeviceId]; ok {
			dst.DeviceName = d.Name
			if i, ok := d.Interfaces[dst.InputPort]; ok {
				dst.InputIntDesc = i.InterfaceDescription
				dst.InputIntAlias = i.SnmpAlias
				dst.InputInterfaceCapacity = i.SnmpSpeedMbps
				dst.InputInterfaceIP = i.InterfaceIP
			}
			if i, ok := d.Interfaces[dst.OutputPort]; ok {
				dst.OutputIntDesc = i.InterfaceDescription
				dst.OutputIntAlias = i.SnmpAlias
				dst.OutputInterfaceCapacity = i.SnmpSpeedMbps
				dst.OutputInterfaceIP = i.InterfaceIP
			}
		}
	}

	// Now the strings.
	smac := make([]byte, 8)
	binary.BigEndian.PutUint64(smac, src.CHF.SrcEthMac())
	dst.SrcEthMac = net.HardwareAddr(smac).String()
	binary.BigEndian.PutUint64(smac, src.CHF.DstEthMac())
	dst.DstEthMac = net.HardwareAddr(smac).String()

	if sft, err := src.CHF.SrcFlowTags(); err != nil {
		dst.SrcFlowTags = sft
	}
	if sft, err := src.CHF.DstFlowTags(); err != nil {
		dst.DstFlowTags = sft
	}
	if sft, err := src.CHF.SrcBgpAsPath(); err != nil {
		dst.SrcBgpAsPath = sft
	}
	if sft, err := src.CHF.DstBgpAsPath(); err != nil {
		dst.DstBgpAsPath = sft
	}
	if sft, err := src.CHF.SrcBgpCommunity(); err != nil {
		dst.SrcBgpCommunity = sft
	}
	if sft, err := src.CHF.DstBgpCommunity(); err != nil {
		dst.DstBgpCommunity = sft
	}

	// Now the addresses.
	var addr net.IP

	// start with the basic src and dst.
	if src.CHF.Ipv4DstAddr() > 0 {
		addr = int2ip(src.CHF.Ipv4DstAddr())
	} else {
		ipr, _ := src.CHF.Ipv6DstAddr()
		addr = net.IP(ipr)
	}
	dst.DstAddr = addr.String()

	// Resolve any hostnames if a resolver is set up.
	if kc.resolver != nil {
		dst.CustomStr["dst_host"] = kc.resolver.Resolve(ctx, dst.DstAddr)
	}

	if src.CHF.Ipv4SrcAddr() > 0 {
		addr = int2ip(src.CHF.Ipv4SrcAddr())
	} else {
		ipr, _ := src.CHF.Ipv6SrcAddr()
		addr = net.IP(ipr)
	}
	dst.SrcAddr = addr.String()

	if kc.resolver != nil {
		dst.CustomStr["src_host"] = kc.resolver.Resolve(ctx, dst.SrcAddr)
	}

	// next hops
	if src.CHF.Ipv4SrcNextHop() > 0 {
		addr = int2ip(src.CHF.Ipv4SrcNextHop())
	} else {
		ipr, _ := src.CHF.Ipv6SrcNextHop()
		addr = net.IP(ipr)
	}
	dst.SrcNextHop = addr.String()

	if src.CHF.Ipv4DstNextHop() > 0 {
		addr = int2ip(src.CHF.Ipv4DstNextHop())
	} else {
		ipr, _ := src.CHF.Ipv6DstNextHop()
		addr = net.IP(ipr)
	}
	dst.DstNextHop = addr.String()

	customs, _ := src.CHF.Custom()
	for i, customsLen := 0, customs.Len(); i < customsLen; i++ {
		cust := customs.At(i)
		val := cust.Value()
		name, ok := kc.mapr.Customs[cust.Id()]

		isInt := false
		if !ok {
			name = strconv.Itoa(int(cust.Id()))
			isInt = true
		}
		switch val.Which() {
		case model.Custom_value_Which_uint16Val:
			dst.CustomInt[name] = int32(val.Uint16Val())
		case model.Custom_value_Which_uint32Val:
			v := val.Uint32Val()
			switch name {
			case "SRC_CDN_INT", "DST_CDN_INT":
				dst.CustomStr[name] = cdn.NameByCDN(v)
			case "TRF_ORIGINATION", "TRF_TERMINATION", "host_direction":
				dst.CustomStr[name] = ic.NETWORK_CLASS_INT_TO_NAME[v]
			case "src_network_bndry", "dst_network_bndry", "ULT_EXIT_NETWORK_BNDRY":
				dst.CustomStr[name] = ic.NameFromNBInt(int(v))
			case "src_connect_type", "dst_connect_type", "ULT_EXIT_CONNECT_TYPE":
				dst.CustomStr[name] = ic.NameFromCTInt(int(v))
			case "dst_rpki":
				if v > ic.RPKI_MAX_NUM || v == ic.RPKI_INVALID {
					dst.CustomStr["i_dst_rpki_name"] = fmt.Sprintf(ic.RPKI_INVALID_NAME, v)
					dst.CustomStr["i_dst_rpki_min_name"] = ic.RPKI_INVALID_MIN_NAME
				} else {
					dst.CustomStr["i_dst_rpki_name"] = ic.RPKI_INT_TO_NAME[v]
					dst.CustomStr["i_dst_rpki_min_name"] = ic.RPKI_INT_TO_MIN_NAME[v]
				}
			case "ULT_EXIT_DEVICE_ID":
				dst.CustomInt[name] = int32(v)
				if _, ok := company[dst.CompanyId]; ok {
					if d, ok := company[dst.CompanyId][kt.DeviceID(v)]; ok {
						dst.CustomStr["ULT_EXIT_DEVICE"] = d.Name
					}
				}
			default:
				if !isInt {
					dst.CustomInt[name] = int32(v) // TODO, way to pull out tags from this?
				}
			}
		case model.Custom_value_Which_uint64Val:
			dst.CustomBigInt[name] = int64(val.Uint64Val())
		case model.Custom_value_Which_strVal:
			sv, _ := val.StrVal()
			dst.CustomStr[name] = sv
		case model.Custom_value_Which_addrVal:
			sv, _ := val.AddrVal()
			var addr net.IP
			if sv[0] == 4 {
				addr = net.IP(sv[1:5])
			} else {
				addr = net.IP(sv[1:])
			}
			dst.CustomStr[name] = addr.String()
		}
	}

	// Finally, update any udr based columns with the correct mapping
	if kc.udrMapr != nil {
		var mapr map[string]*UDR
		if kc.udrMapr.Subtype != nil {
			mapr = kc.udrMapr.Subtype
		} else if ap, ok := dst.CustomInt[APP_PROTOCOL_COL]; ok {
			if maprr, ok := kc.udrMapr.UDRs[ap]; ok {
				mapr = maprr
			}
		}
		for col, udr := range mapr {
			switch udr.Type {
			case UDR_TYPE_INT:
				if val, ok := dst.CustomInt[col]; ok {
					dst.CustomInt[udr.ColumnName] = val
					delete(dst.CustomInt, col)
				}
			case UDR_TYPE_STRING:
				if val, ok := dst.CustomStr[col]; ok {
					dst.CustomStr[udr.ColumnName] = val
					delete(dst.CustomStr, col)
				}
			case UDR_TYPE_BIGINT:
				if val, ok := dst.CustomBigInt[col]; ok {
					dst.CustomBigInt[udr.ColumnName] = val
					delete(dst.CustomBigInt, col)
				}
			}
			if _, ok := dst.CustomStr[UDR_TYPE]; !ok {
				dst.CustomStr[UDR_TYPE] = udr.ApplicationName
			}
		}
	}

	// Set the type dynamically here to help out processing.
	dst.EventType = kc.getEventType(dst)
	dst.Provider = kc.getProviderType(dst)

	return nil
}

func int2ip(nn uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, nn)
	return ip
}

func lookupCityName(citycache map[uint32]string, code uint32, env *lmdb.Env) string {
	if n, ok := citycache[code]; ok {
		return n
	}
	n := lookupGeoName(code, env)
	citycache[code] = n
	return n
}

func lookupRegionName(regioncache map[uint32]string, code uint32, env *lmdb.Env) string {
	if n, ok := regioncache[code]; ok {
		return n
	}
	n := lookupGeoName(code, env)
	regioncache[code] = n
	return n
}

func lookupGeoName(code uint32, env *lmdb.Env) string {
	if env != nil {
		txn, err := env.BeginTxn(nil, lmdb.Readonly)
		if err != nil {
			return ""
		}
		defer txn.Abort()

		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return ""
		}
		defer env.CloseDBI(dbi)

		key := []byte(strconv.FormatUint(uint64(code), 10))
		v, err := txn.Get(dbi, key)
		switch {
		case lmdb.IsNotFound(err):
			return ""
		case err != nil:
			return ""
		}
		return string(v)
	}

	return ""
}
