package main

/*
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <sassrv/rv7api.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>
*/
import "C"

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

var (
	serviceStr *string = flag.String("service", "", "service parameter")
	networkStr *string = flag.String("network", "", "network parameter")
	daemonStr  *string = flag.String("daemon", "", "daemon parameter")
)

// fieldSpec captures one "-field name=tz:value" command-line argument.
//
//	type 'i' (signed int)    size 1, 2, 4, 8
//	type 'u' (unsigned int)  size 1, 2, 4, 8
//	type 'f' (float)         size 4, 8
//	type 's' (string)        size 0 (length comes from strlen)
//	type 'o' (opaque bytes)  size = number of bytes; value is 2*size hex digits
type fieldSpec struct {
	name  string
	typ   byte
	size  int
	value string
}

// fieldList implements flag.Value to collect repeated -field arguments.
type fieldList []fieldSpec

func (fl *fieldList) String() string {
	return fmt.Sprintf("%v", []fieldSpec(*fl))
}

func (fl *fieldList) Set(spec string) error {
	f, err := parseFieldSpec(spec)
	if err != nil {
		return err
	}
	*fl = append(*fl, f)
	return nil
}

var fields fieldList

func init() {
	flag.Var(&fields, "field",
		"name=tz:value field to add to each msg (repeatable). "+
			"t in i,u,s,o,f. z is size in bytes (ints: 1/2/4/8; floats: 4/8; "+
			"strings: 0; opaque: byte count). Examples: SEQ_NO=i2:12 PRICE=f8:1.25 "+
			"TAG=s0:hello BLOB=o4:DEADBEEF")
}

func parseFieldSpec(spec string) (fieldSpec, error) {
	var f fieldSpec

	eq := strings.IndexByte(spec, '=')
	if eq <= 0 {
		return f, fmt.Errorf("missing '=' in field spec %q", spec)
	}
	f.name = spec[:eq]

	rest := spec[eq+1:]
	if len(rest) < 1 {
		return f, fmt.Errorf("empty type/value in field spec %q", spec)
	}
	f.typ = rest[0]
	switch f.typ {
	case 'i', 'u', 's', 'o', 'f':
	default:
		return f, fmt.Errorf("unknown type %q in field spec %q (expected i/u/s/o/f)", f.typ, spec)
	}

	colon := strings.IndexByte(rest, ':')
	if colon < 0 {
		return f, fmt.Errorf("missing ':' before value in field spec %q", spec)
	}
	sizeStr := rest[1:colon]
	if sizeStr == "" {
		f.size = 0
	} else {
		sz, err := strconv.Atoi(sizeStr)
		if err != nil || sz < 0 || sz > 65535 {
			return f, fmt.Errorf("invalid size %q in field spec %q", sizeStr, spec)
		}
		f.size = sz
	}
	f.value = rest[colon+1:]

	switch f.typ {
	case 'i', 'u':
		if f.size != 1 && f.size != 2 && f.size != 4 && f.size != 8 {
			return f, fmt.Errorf("int size must be 1/2/4/8 (got %d) in field spec %q", f.size, spec)
		}
	case 'f':
		if f.size != 4 && f.size != 8 {
			return f, fmt.Errorf("float size must be 4 or 8 (got %d) in field spec %q", f.size, spec)
		}
	case 's':
		// size 0 means "use strlen"; accept anything else but treat the
		// string value as null-terminated for AddStringEx.
	case 'o':
		if f.size <= 0 {
			return f, fmt.Errorf("opaque size must be >= 1 in field spec %q", spec)
		}
	}
	return f, nil
}

// applyField writes one field to a message using the type-dispatched Add*Ex
// call.  Returns the tibrv_status from the underlying call.
func applyField(msg C.tibrvMsg, f *fieldSpec) C.tibrv_status {
	nameC := C.CString(f.name)
	defer C.free(unsafe.Pointer(nameC))

	switch f.typ {
	case 'i':
		v, err := strconv.ParseInt(f.value, 0, 64)
		if err != nil {
			return C.TIBRV_INVALID_ARG
		}
		switch f.size {
		case 1:
			return C.tibrvMsg_AddI8Ex(msg, nameC, C.tibrv_i8(v), 0)
		case 2:
			return C.tibrvMsg_AddI16Ex(msg, nameC, C.tibrv_i16(v), 0)
		case 4:
			return C.tibrvMsg_AddI32Ex(msg, nameC, C.tibrv_i32(v), 0)
		case 8:
			return C.tibrvMsg_AddI64Ex(msg, nameC, C.tibrv_i64(v), 0)
		}
	case 'u':
		v, err := strconv.ParseUint(f.value, 0, 64)
		if err != nil {
			return C.TIBRV_INVALID_ARG
		}
		switch f.size {
		case 1:
			return C.tibrvMsg_AddU8Ex(msg, nameC, C.tibrv_u8(v), 0)
		case 2:
			return C.tibrvMsg_AddU16Ex(msg, nameC, C.tibrv_u16(v), 0)
		case 4:
			return C.tibrvMsg_AddU32Ex(msg, nameC, C.tibrv_u32(v), 0)
		case 8:
			return C.tibrvMsg_AddU64Ex(msg, nameC, C.tibrv_u64(v), 0)
		}
	case 'f':
		v, err := strconv.ParseFloat(f.value, 64)
		if err != nil {
			return C.TIBRV_INVALID_ARG
		}
		switch f.size {
		case 4:
			return C.tibrvMsg_AddF32Ex(msg, nameC, C.tibrv_f32(v), 0)
		case 8:
			return C.tibrvMsg_AddF64Ex(msg, nameC, C.tibrv_f64(v), 0)
		}
	case 's':
		valC := C.CString(f.value)
		defer C.free(unsafe.Pointer(valC))
		return C.tibrvMsg_AddStringEx(msg, nameC, valC, 0)
	case 'o':
		buf, err := hex.DecodeString(f.value)
		if err != nil {
			return C.TIBRV_INVALID_ARG
		}
		if len(buf) != f.size {
			return C.TIBRV_INVALID_ARG
		}
		var ptr unsafe.Pointer
		if len(buf) > 0 {
			ptr = unsafe.Pointer(&buf[0])
		}
		return C.tibrvMsg_AddOpaqueEx(msg, nameC, ptr, C.tibrv_u32(f.size), 0)
	}
	return C.TIBRV_INVALID_ARG
}

// applyDefaultFields writes the historical built-in field set that the
// original C and Go versions hard-coded.  Used only when no -field is given.
func applyDefaultFields(msg C.tibrvMsg) C.tibrv_status {
	type def struct {
		name string
		typ  byte
		size int
		val  string
	}
	defaults := []def{
		{"MSG_TYPE", 'i', 2, "1"},
		{"REC_TYPE", 'i', 2, "5009"},
		{"SEQ_NO", 'i', 2, "1"},
		{"REC_STATUS", 'i', 2, "0"},
	}
	for _, d := range defaults {
		f := fieldSpec{name: d.name, typ: d.typ, size: d.size, value: d.val}
		if err := applyField(msg, &f); err != C.TIBRV_OK {
			return err
		}
	}
	return C.TIBRV_OK
}

func usage() {
	fmt.Fprintf(os.Stderr,
		"Usage: %s [-service service] [-network network] [-daemon daemon]\n"+
			"          [-field name=tz:value]... subject_list\n",
		os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	subjects := flag.Args()

	if len(subjects) == 0 {
		usage()
	}

	var err C.tibrv_status
	var transport C.tibrvTransport

	progname := C.CString(os.Args[0])
	defer C.free(unsafe.Pointer(progname))

	// Convert Go strings to C strings for the transport creation
	var serviceC, networkC, daemonC *C.char
	if *serviceStr != "" {
		serviceC = C.CString(*serviceStr)
		defer C.free(unsafe.Pointer(serviceC))
	}
	if *networkStr != "" {
		networkC = C.CString(*networkStr)
		defer C.free(unsafe.Pointer(networkC))
	}
	if *daemonStr != "" {
		daemonC = C.CString(*daemonStr)
		defer C.free(unsafe.Pointer(daemonC))
	}

	err = C.tibrv_Open()
	if err != C.TIBRV_OK {
		fmt.Fprintf(os.Stderr, "%s: Failed to open TIB/Rendezvous: %s\n",
			os.Args[0], C.GoString(C.tibrvStatus_GetText(err)))
		os.Exit(1)
	}

	err = C.tibrvTransport_Create(&transport, serviceC, networkC, daemonC)
	if err != C.TIBRV_OK {
		fmt.Fprintf(os.Stderr, "%s: Failed to initialize transport: %s\n",
			os.Args[0], C.GoString(C.tibrvStatus_GetText(err)))
		os.Exit(1)
	}

	C.tibrvTransport_SetDescription(transport, progname)

	for _, subject := range subjects {
		fmt.Printf("pubrv7test.go: Publishing to subject %s\n", subject)

		var pubMsg C.tibrvMsg
		C.tibrvMsg_Create(&pubMsg)

		pubSubject := fmt.Sprintf("_TIC.%s", subject)
		pubSubjectC := C.CString(pubSubject)
		C.tibrvMsg_SetSendSubject(pubMsg, pubSubjectC)

		err = applyDefaultFields(pubMsg)
		if err == C.TIBRV_OK {
			for i := range fields {
				if e := applyField(pubMsg, &fields[i]); e != C.TIBRV_OK {
					fmt.Fprintf(os.Stderr,
						"%s: failed to add field %s (type=%c size=%d): %s\n",
						os.Args[0], fields[i].name, fields[i].typ,
						fields[i].size, C.GoString(C.tibrvStatus_GetText(e)))
					err = e
					break
				}
			}
		}
		if err != C.TIBRV_OK {
			C.tibrvMsg_Destroy(pubMsg)
			C.free(unsafe.Pointer(pubSubjectC))
			os.Exit(2)
		}

		err = C.tibrvTransport_Send(transport, pubMsg)
		C.tibrvMsg_Destroy(pubMsg)
		C.free(unsafe.Pointer(pubSubjectC))

		if err != C.TIBRV_OK {
			fmt.Fprintf(os.Stderr, "%s: Error %s publishing to \"%s\"\n",
				os.Args[0], C.GoString(C.tibrvStatus_GetText(err)), subject)
			os.Exit(2)
		}
	}

	C.tibrv_Close()
}
