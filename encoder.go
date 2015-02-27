package dendrite

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"github.com/bububa/dendrite/logs"
)

type Encoder interface {
	Encode(out map[string]Column, writer io.Writer)
}

type JsonEncoder struct{}
type StatsdEncoder struct{}
type RawStringEncoder struct{}

func NewEncoder(u *url.URL) (Encoder, error) {
	a := strings.Split(u.Scheme, "+")
	switch a[len(a)-1] {
	case "json":
		return new(JsonEncoder), nil
	case "statsd":
		return new(StatsdEncoder), nil
	}
	return new(RawStringEncoder), nil
}

func (*RawStringEncoder) Encode(out map[string]Column, writer io.Writer) {
	for _, v := range out {
		if v.Type == String {
			writer.Write([]byte(v.Value.(string) + "\n"))
		}
	}
}

func (*JsonEncoder) Encode(out map[string]Column, writer io.Writer) {
	stripped := make(map[string]interface{})
	for k, v := range out {
		stripped[k] = v.Value
	}
	bytes, err := json.Marshal(stripped)
	if err != nil {
		panic(err)
	}
	bytes = append(bytes, '\n')
		logs.Debug("Type")
	writer.Write(bytes)
}

func (*StatsdEncoder) Encode(out map[string]Column, writer io.Writer) {
	for k, v := range out {
		switch v.Treatment {
		case Gauge:
			writer.Write([]byte(fmt.Sprintf("%s:%d|g", k, v.Value)))
		case Metric:
			timing := v.Value.(float64)*1000
			fmt.Println(fmt.Sprintf("Timing: %s", timing))
			writer.Write([]byte(fmt.Sprintf("%s-us:%f|ms", k, timing)))
		case Counter:
			writer.Write([]byte(fmt.Sprintf("%s:%d|c", k, v.Value)))
		}
	}
}
