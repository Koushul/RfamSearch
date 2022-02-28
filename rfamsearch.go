package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/colorstring"
	"github.com/mitchellh/mapstructure"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	ID          int
	Sequence    string
	Status      State
	LastChecked time.Time
	HTTPDesc    string
	JobId       string `json:"jobId"`
	Opened      string `json:"opened"`
	ResultURL   string `json:"resultURL"`
	httpClient  *http.Client
	Results     Results
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
	201: "Submitted",
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

	j.HTTPDesc = serverResponses[res.StatusCode]

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
	j.LastChecked = time.Now()

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

var wg sync.WaitGroup

//Creates new Job from a DNA sequence
func jobMaker(workerId int, httpClient *http.Client, sequences <-chan string, newJobs chan<- Job) {
	for s := range sequences {
		j := Job{
			ID:         len(sequences),
			Status:     Ready,
			Sequence:   s,
			httpClient: httpClient,
		}

		newJobs <- j
	}

}

func jobSubmitter(workerId int, newJobs chan Job, submittedJobs chan<- Job) {
	for j := range newJobs {
		j.submit()
		fmt.Printf("[%v] Submitted new Sequence. JobID: %v\n", workerId, j.JobId)
		submittedJobs <- j
	}
}

func resultsFetcher(workerId int, nseqs int, submittedJobs chan Job, finishedJobs []Job) {
	for j := range submittedJobs {
		if time.Since(j.LastChecked) > time.Second*10 {
			j.getResults()
		}

		if j.Status == Completed {
			colorstring.Print(DNAColorize(j.Results.searchSequence))
			fmt.Println("\t", j.Results.rna)
			tmpJob := j
			finishedJobs[j.ID] = tmpJob
			wg.Done()

		} else {
			//if job isn't completed, send it back to the channel
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

	//initialize shared http client
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	Client := &http.Client{
		Timeout:   time.Second * 30,
		Transport: t,
	}

	tick := time.Now()

	filename := os.Args[1]
	numWorkers, err := strconv.Atoi(os.Args[2])

	if err != nil {
		panic(err)
	}

	//read fasta file with input sequences
	seqs := readFasta(filename)
	fmt.Printf("Created %v sequences.\n", len(seqs))

	var finishedJobs = make([]Job, len(seqs))

	//make channels
	sequences := make(chan string, len(seqs))
	newJobs := make(chan Job, len(seqs))
	pendingJobs := make(chan Job, len(seqs))

	for w := 1; w <= numWorkers; w++ {
		go jobMaker(w, Client, sequences, newJobs)
		go jobSubmitter(w, newJobs, pendingJobs)
		go resultsFetcher(w, len(seqs), pendingJobs, finishedJobs)
	}

	for _, s := range seqs {
		sequences <- s
		wg.Add(1)
	}
	close(sequences)

	wg.Wait()

	elapsed := time.Since(tick)

	fmt.Printf("Completed %v jobs in %s\n", len(finishedJobs), elapsed)

	// fmt.Println(finishedJobs)

}

//TODO: Add Progressbar
//TODO: Save results to file
//TODO: Refactor folder structures
