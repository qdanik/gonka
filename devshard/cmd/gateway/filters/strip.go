package filters

import (
	"encoding/json"
	"errors"
	"slices"
)

// errMalformedJSON ends a scan; the caller then passes the body through untouched.
var errMalformedJSON = errors.New("filters: malformed json")

// stripScanner removes clientStrippedFields at any depth in one pass, copying the bytes it keeps rather
// than decoding them, so every number it passes through survives exactly as the host wrote it. See
// gateway-request-filtering.md, "The response side".
type stripScanner struct {
	input   []byte
	pos     int
	out     []byte
	changed bool
}

func stripFields(payload []byte) ([]byte, bool, error) {
	return appendStripped(make([]byte, 0, len(payload)), payload)
}

// appendStripped writes the stripped payload onto dst, so a caller rebuilding an SSE event around it
// pays one allocation instead of one for the payload and another for the event.
func appendStripped(dst, payload []byte) ([]byte, bool, error) {
	scanner := &stripScanner{input: payload, out: dst}
	scanner.skipSpace()
	if err := scanner.value(); err != nil {
		return nil, false, err
	}
	scanner.skipSpace()
	if scanner.pos != len(scanner.input) {
		return nil, false, errMalformedJSON
	}
	return scanner.out, scanner.changed, nil
}

func (s *stripScanner) skipSpace() {
	for s.pos < len(s.input) {
		switch s.input[s.pos] {
		case ' ', '\t', '\n', '\r':
			s.pos++
		default:
			return
		}
	}
}

func (s *stripScanner) value() error {
	if s.pos >= len(s.input) {
		return errMalformedJSON
	}
	switch s.input[s.pos] {
	case '{':
		return s.object()
	case '[':
		return s.array()
	case '"':
		raw, err := s.stringValue()
		if err != nil {
			return err
		}
		s.out = append(s.out, raw...)
		return nil
	default:
		return s.scalar()
	}
}

// scalar copies a number, true, false or null verbatim, so 9007199254740993 stays itself.
func (s *stripScanner) scalar() error {
	start := s.pos
	for s.pos < len(s.input) {
		switch s.input[s.pos] {
		case ',', '}', ']', ' ', '\t', '\n', '\r':
			if s.pos == start {
				return errMalformedJSON
			}
			s.out = append(s.out, s.input[start:s.pos]...)
			return nil
		default:
			s.pos++
		}
	}
	if s.pos == start {
		return errMalformedJSON
	}
	s.out = append(s.out, s.input[start:s.pos]...)
	return nil
}

// stringValue returns the string with its quotes and escapes intact, since anything it copies goes out
// to the client byte for byte.
func (s *stripScanner) stringValue() ([]byte, error) {
	if s.pos >= len(s.input) || s.input[s.pos] != '"' {
		return nil, errMalformedJSON
	}
	start := s.pos
	s.pos++
	for s.pos < len(s.input) {
		switch s.input[s.pos] {
		case '\\':
			s.pos += 2
		case '"':
			s.pos++
			return s.input[start:s.pos], nil
		default:
			s.pos++
		}
	}
	return nil, errMalformedJSON
}

func (s *stripScanner) array() error {
	s.pos++
	s.out = append(s.out, '[')
	s.skipSpace()
	if s.pos < len(s.input) && s.input[s.pos] == ']' {
		s.pos++
		s.out = append(s.out, ']')
		return nil
	}
	for {
		if err := s.value(); err != nil {
			return err
		}
		s.skipSpace()
		if s.pos >= len(s.input) {
			return errMalformedJSON
		}
		switch s.input[s.pos] {
		case ',':
			s.pos++
			s.out = append(s.out, ',')
			s.skipSpace()
		case ']':
			s.pos++
			s.out = append(s.out, ']')
			return nil
		default:
			return errMalformedJSON
		}
	}
}

func (s *stripScanner) object() error {
	s.pos++
	s.out = append(s.out, '{')
	s.skipSpace()
	if s.pos < len(s.input) && s.input[s.pos] == '}' {
		s.pos++
		s.out = append(s.out, '}')
		return nil
	}
	kept := false
	for {
		rawKey, err := s.stringValue()
		if err != nil {
			return err
		}
		s.skipSpace()
		if s.pos >= len(s.input) || s.input[s.pos] != ':' {
			return errMalformedJSON
		}
		s.pos++
		s.skipSpace()

		if isStrippedKey(rawKey) {
			if err := s.skipValue(); err != nil {
				return err
			}
			s.changed = true
		} else {
			if kept {
				s.out = append(s.out, ',')
			}
			s.out = append(s.out, rawKey...)
			s.out = append(s.out, ':')
			if err := s.value(); err != nil {
				return err
			}
			kept = true
		}

		s.skipSpace()
		if s.pos >= len(s.input) {
			return errMalformedJSON
		}
		switch s.input[s.pos] {
		case ',':
			s.pos++
			s.skipSpace()
		case '}':
			s.pos++
			s.out = append(s.out, '}')
			return nil
		default:
			return errMalformedJSON
		}
	}
}

// skipValue advances past a value without copying it; it must accept everything value does.
func (s *stripScanner) skipValue() error {
	if s.pos >= len(s.input) {
		return errMalformedJSON
	}
	switch s.input[s.pos] {
	case '"':
		_, err := s.stringValue()
		return err
	case '{', '[':
		return s.skipContainer()
	default:
		start := s.pos
		for s.pos < len(s.input) {
			switch s.input[s.pos] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				if s.pos == start {
					return errMalformedJSON
				}
				return nil
			default:
				s.pos++
			}
		}
		if s.pos == start {
			return errMalformedJSON
		}
		return nil
	}
}

func (s *stripScanner) skipContainer() error {
	depth := 0
	for s.pos < len(s.input) {
		switch s.input[s.pos] {
		case '"':
			if _, err := s.stringValue(); err != nil {
				return err
			}
			continue
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				s.pos++
				return nil
			}
		}
		s.pos++
	}
	return errMalformedJSON
}

// isStrippedKey decodes the key only when it carries a backslash: "logpro\u0062" spells a stripped
// field too, and comparing raw bytes alone would hand a host that trick past the filter.
func isStrippedKey(rawKey []byte) bool {
	if !slices.Contains(rawKey, '\\') {
		if len(rawKey) < 2 {
			return false
		}
		return slices.ContainsFunc(clientStrippedFields, func(field string) bool {
			return string(rawKey[1:len(rawKey)-1]) == field
		})
	}
	var decoded string
	if err := json.Unmarshal(rawKey, &decoded); err != nil {
		return false
	}
	return slices.Contains(clientStrippedFields, decoded)
}
