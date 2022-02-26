package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mitchellh/colorstring"
	"github.com/mitchellh/mapstructure"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	Sequence      string
	Status        State
	JobId         string `json:"jobId"`
	Opened        string `json:"opened"`
	EstimatedTime string `json:"estimatedTime"`
	ResultURL     string `json:"resultURL"`
	httpClient    *http.Client
	Results       Results
}

type State int64

const (
	Completed State = iota
	Created
	Submitted
	Pending
	Error
)

type Results struct {
	closed                string
	searchSequence        string
	opened                string
	numHits               float64
	started               string
	jobId                 string
	rna                   string
	coloredSearchSequence string
	rnaMatch              []RNAMatch
}

type RNAMatch struct {
	Score  string `json:"score"`
	E      string `json:"E"`
	Acc    string `json:"acc"`
	End    string `json:"end"`
	Strand string `json:"strand"`
	Id     string `json:"id"`
	GC     string `json:"GC"`
	Start  string `json:"start"`

	Alignment struct {
		User_seq string `json:"user_seq"`
		Hit_seq  string `json:"hit_seq"`
		Ss       string `json:"ss"`
		Match    string `json:"match"`
		Pp       string `json:"pp"`
		Nc       string `json:"nc"`
	}
}

func (j *Job) submit() {

	data := url.Values{}
	data.Set("seq", j.Sequence)

	r, _ := http.NewRequest("POST", rfamSequenceSearchEndpoint, strings.NewReader(data.Encode()))

	r.Header.Add("Expect", "")
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Accept", "application/json")

	res, err := j.httpClient.Do(r)

	if err != nil {
		panic(err)
	}

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	err2 := json.Unmarshal(body, &j)
	if err2 != nil {
		panic(err2)
	}

	j.Status = Submitted

}

func (j *Job) getResults() {

	r, err := http.NewRequest("GET", j.ResultURL, strings.NewReader(url.Values{}.Encode()))
	if err != nil {
		panic(err)
	}

	r.Header.Add("Expect", "")
	r.Header.Add("Accept", "application/json")

	res, getErr := j.httpClient.Do(r)

	defer r.Body.Close()

	if getErr != nil {
		panic(getErr)
	}

	body, _ := ioutil.ReadAll(res.Body)

	defer res.Body.Close()

	results := &j.Results
	var f map[string]interface{}

	err2 := json.Unmarshal(body, &f)

	if err2 != nil {
		panic(err2)
	}

	for k, v := range f {
		switch k {
		case "status":
			j.Status = Pending
		case "closed":
			results.closed = v.(string)
		case "searchSequence":
			results.searchSequence = v.(string)
		case "opened":
			results.opened = v.(string)
		case "started":
			results.started = v.(string)
		case "numHits":
			results.numHits = v.(float64)
		case "jobId":
			results.jobId = v.(string)
		case "hits":
			config := &mapstructure.DecoderConfig{}
			for rna, desc := range v.(map[string]interface{}) {
				results.rna = rna
				for i := 0; i < len(desc.([]interface{})); i++ {
					m := desc.([]interface{})[i]
					var rm RNAMatch
					config.Result = &rm
					decoder, _ := mapstructure.NewDecoder(config)
					decoder.Decode(m)
					results.rnaMatch = append(results.rnaMatch, rm)
				}
			}
		}
	}

	if results.closed != "" {
		j.Status = Completed
	}

}

func DNAColorize(s string) string {
	replacer := strings.NewReplacer("A", "[red]C", "T", "[blue]T", "G", "[green]G", "C", "[yellow]C")
	return replacer.Replace(s)
}

func main() {

	defaultTimeout := time.Second * 10

	var seqs [3]string

	seqs[0] = "GGGGGATTAGCTCAGTTTGGGAGAGCGCCTGCTTTGCACGCAGGAGGTCAGCGGTTCGAGCCCGCTATCCTCCAC"
	seqs[1] = "CGGGAATAGCTCAGTTGGCTAGAGCATCAGCCTTCCAAGCTGAGGGTCGCGGGTTCGAGCCCCGTTTCCCGCTC"
	seqs[2] = "TGGGGTATCGCCAAGCGGTAAGGCACCTGGTTTTGGTCCAGGCATTCCGAGGTTCGAATCCTTGTACCCCAGCCA"

	var jobs []Job

	for _, s := range seqs {
		j := Job{
			Status:     Created,
			Sequence:   s,
			httpClient: &http.Client{Timeout: defaultTimeout},
		}
		jobs = append(jobs, j)
	}

	completes := 0

	for completes != 3 {
		for idx := range jobs {
			job := &jobs[idx]

			switch job.Status {
			case Created:
				fmt.Println("Submitting...")
				job.submit()
			case Submitted:
				job.getResults()
			case Error:
				fmt.Println("Some Jobs failed")
			case Pending:
				job.getResults()
			case Completed:
				completes++
				colorstring.Print(DNAColorize(job.Results.searchSequence))
				fmt.Println("\t", job.Results.rna)
			}
			time.Sleep(time.Second * 10)

		}
	}

	fmt.Println(jobs)

}
