package dendrite

import (
	"crypto/sha1"
	"encoding/base64"
	"github.com/bububa/dendrite/logs"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
)

type Column struct {
	Type      FieldType
	Treatment FieldTreatment
	Value     interface{}
}

type Record map[string]Column

type Parser interface {
	Consume(bytes []byte, counter *int64)
}

type RegexpParser struct {
	group       string
	compiled    *regexp.Regexp
	output      chan Record
	buffer      []byte
	fields      []FieldConfig
	file        string
	hostname    string
	maxLineSize int
}

func NewRegexpParser(hostname string, group string, file string, output chan Record, pattern string, fields []FieldConfig, maxLineSize int64) Parser {
	parser := new(RegexpParser)
	parser.maxLineSize = int(maxLineSize)
	parser.hostname = hostname
	parser.file = file
	parser.group = group
	parser.output = output
	parser.buffer = make([]byte, 0)
	re, err := regexp.Compile(pattern)
	if err != nil {
		panic(err)
	} else {
		parser.compiled = re
		for i, name := range re.SubexpNames() {
			if name != "" {
				found := false
				for n, spec := range fields {
					if spec.Name == "" {
						spec.Name = spec.Alias
					}
					if name == spec.Name {
						found = true
						fields[n].Group = i
						logs.Debug("setting group alias: %s, name: %s, group: %d", spec.Alias, spec.Name, spec.Group)
					}
				}
				if !found {
					var spec FieldConfig
					spec.Group = i
					spec.Alias = name
					spec.Type = String
					fields = append(fields, spec)
				}
			}
		}
	}
	parser.fields = fields
	for _, f := range parser.fields {
		logs.Debug("p.f: alias: %s, name: %s, group: %d, type: %d", f.Alias, f.Name, f.Group, f.Type)
	}
	return parser
}

func (parser *RegexpParser) Consume(bytes []byte, counter *int64) {
	parser.buffer = append(parser.buffer, bytes...)
	logs.Debug("consuming %d bytes of %s", len(bytes), parser.file)
	l := len(parser.buffer)
	if l > parser.maxLineSize {
		off := l - parser.maxLineSize
		logs.Debug("chopping %d bytes off buffer (was: %d, max: %d)", off, l, parser.maxLineSize)
		atomic.AddInt64(counter, int64(off))
		parser.buffer = parser.buffer[off:]
	}
	for {
		m := parser.compiled.FindSubmatchIndex(parser.buffer)
		if m == nil {
			return
		}

		hasher := sha1.New()

		out := make(map[string]Column)
		out["_offset"] = Column{Integer, Simple, atomic.LoadInt64(counter)}
		out["_file"] = Column{String, Simple, parser.file}
		out["_time"] = Column{Timestamp, Simple, StandardTimeProvider.Now().Unix()}
		out["_group"] = Column{String, Simple, parser.group}
		out["_hostname"] = Column{String, Simple, parser.hostname}
		for _, spec := range parser.fields {
			g := spec.Group
			if g < 0 || g > len(m)/2 {
				logs.Error("spec group out of range: alias: %s, name: %s, g: %d", spec.Alias, spec.Name, g)
				panic(-1)
			}
			if m[g*2] == -1 {
				continue
			}
			s := string(parser.buffer[m[g*2]:m[g*2+1]])
			switch spec.Type {
			case Timestamp:
				t, err := time.Parse(spec.Format, s)
				if err != nil {
					logs.Warn("date parse error: %s", err)
				} else {
					if t.Year() == 0 {
						now := StandardTimeProvider.Now()
						adjusted := time.Date(now.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
						if adjusted.After(now) {
							adjusted = time.Date(now.Year()-1, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
						}
						t = adjusted
					}
					out[spec.Alias] = Column{Timestamp, spec.Treatment, t.Unix()}
				}
			case String:
				if spec.Treatment == Tokens {
					out[spec.Alias] = Column{Tokens, spec.Treatment, spec.Pattern.FindAllString(s, -1)}
				} else if spec.Treatment == Hash {
					hasher.Reset()
					hasher.Write([]byte(spec.Salt))
					hasher.Write([]byte(s))
					sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
					out[spec.Alias] = Column{Tokens, spec.Treatment, sha}
				} else {
					out[spec.Alias] = Column{String, spec.Treatment, s}
				}
			case Integer:
				n, err := strconv.ParseInt(s, 10, 64)
				if err == nil {
					out[spec.Alias] = Column{spec.Type, spec.Treatment, n}
				}
			case Double:
				n, err := strconv.ParseFloat(s, 64)
				if err == nil {
					out[spec.Alias] = Column{spec.Type, spec.Treatment, n}
				}

			default:
				panic(nil)
			}
		}
		parser.output <- out
		atomic.AddInt64(counter, int64(m[1]))

		parser.buffer = parser.buffer[m[1]:]
	}
	logs.Debug("done with %s", parser.file)
}
