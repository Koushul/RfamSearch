package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mitchellh/colorstring"
	"github.com/mitchellh/mapstructure"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	Sequence      string
	Status        State
	HTTPDesc      string
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
	Ready
	Submitted
	Pending
	Failed
)

type Results struct {
	closed         string
	searchSequence string
	opened         string
	numHits        float64
	started        string
	jobId          string
	rna            string
	rnaMatch       []RNAMatch
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

var serverResponses = map[int]string{
	202: "Accepted",
	502: "Bad gateway",
	503: "Service unavailable",
	200: "OK",
	410: "Gone",
	510: "Service unvailable",
	500: "Internal server error",
}

func (j *Job) submit() {

	data := url.Values{}
	data.Set("seq", j.Sequence)

	r, _ := http.NewRequest(http.MethodPost, rfamSequenceSearchEndpoint, strings.NewReader(data.Encode()))

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

	r, err := http.NewRequest(http.MethodGet, j.ResultURL, strings.NewReader(url.Values{}.Encode()))
	if err != nil {
		panic(err)
	}

	r.Header.Add("Expect", "")
	r.Header.Add("Accept", "application/json")

	res, getErr := j.httpClient.Do(r)
	j.HTTPDesc = serverResponses[res.StatusCode]

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
	replacer := strings.NewReplacer("A", "[red]A", "T", "[blue]T", "G", "[green]G", "C", "[yellow]C")
	return replacer.Replace(s)
}

//Creates new Job from a DNA sequence
func jobMaker(workerId int, httpClient *http.Client, sequences <-chan string, newJobs chan<- Job) {
	for s := range sequences {
		j := Job{
			Status:     Ready,
			Sequence:   s,
			httpClient: httpClient,
		}

		newJobs <- j
	}

}

func jobSubmitter(workerId int, newJobs <-chan Job, submittedJobs chan<- Job) {
	for j := range newJobs {
		j.submit()
		fmt.Printf("[%v] Submitted new Sequence\n", workerId)

		submittedJobs <- j
	}
}

func resultsFetcher(workerId int, submittedJobs chan Job, completedJobs chan<- Job) {
	for j := range submittedJobs {
		time.Sleep(time.Second * 5)
		j.getResults()
		if j.Status == Completed {
			fmt.Printf("[%v] Completed!\n", workerId)
			completedJobs <- j
		} else {
			fmt.Printf("[%v] Job still running...\n", workerId)
			submittedJobs <- j
		}
	}

}

func readFasta(filename string) []string {
	var seqs []string

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	data := strings.Split(string(file), ">")

	for _, entry := range data[1:] {
		sq := strings.Split(entry, "\n")
		seqs = append(seqs, sq[1])
	}

	return seqs
}

func main() {

	defaultTimeout := time.Second * 10

	seqs := readFasta(os.Args[1])

	//make channels
	sequences := make(chan string, len(seqs))
	newJobs := make(chan Job, len(seqs))
	submittedJobs := make(chan Job, len(seqs))
	completedJobs := make(chan Job, len(seqs))

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	httpClient := &http.Client{
		Timeout:   defaultTimeout,
		Transport: t,
	}

	for w := 1; w <= 5; w++ {
		go jobMaker(w, httpClient, sequences, newJobs)
		go jobSubmitter(w, newJobs, submittedJobs)
		go resultsFetcher(w, submittedJobs, completedJobs)
	}

	for _, s := range seqs {
		sequences <- s
	}
	close(sequences)

	var finishedJobs = make([]Job, len(seqs))

	for cj := range completedJobs {
		colorstring.Println(DNAColorize(cj.Results.searchSequence))
		finishedJobs = append(finishedJobs, cj)
		if len(finishedJobs) == len(seqs) {
			close(completedJobs)
			break
		}
	}

}
