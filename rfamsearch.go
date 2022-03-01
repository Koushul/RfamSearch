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

	"github.com/gosuri/uilive"
	"github.com/mitchellh/mapstructure"
	"github.com/schollz/progressbar/v3"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	ID          int    //unique job id, also serves as index for the final output array
	Sequence    string //sequence to search Rfam for
	Status      State
	LastChecked time.Time //last time the job status was checked via a get request
	HTTPDesc    string    //last server response. used for tracking errors
	JobId       string    `json:"jobId"`
	Opened      string    `json:"opened"`
	ResultURL   string    `json:"resultURL"`
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

//Submit a sequence to Rfam
func (j *Job) submit() {

	data := url.Values{}
	data.Set("seq", j.Sequence)

	r, _ := http.NewRequest(http.MethodPost, rfamSequenceSearchEndpoint, strings.NewReader(data.Encode()))
	r.Close = true
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

//Check if Job has finished running and grab the results
func (j *Job) getResults() {
	r, err := http.NewRequest(http.MethodGet, j.ResultURL, strings.NewReader(url.Values{}.Encode()))
	r.Close = true

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

// func jobSubmitter(newJobs chan Job, pendingJobs chan<- Job) {
// 	for j := range newJobs {
// 		j.submit()
// 		fmt.Printf("Submitted new Sequence %v. JobID: %v\n", j.ID, j.JobId)
// 		pendingJobs <- j
// 		time.Sleep(time.Second * 5)
// 	}
// }

func resultsFetcher(pendingJobs chan Job, finishedJobs []Job, bar *progressbar.ProgressBar) {
	for j := range pendingJobs {

		if time.Since(j.LastChecked) > time.Second*5 {
			j.getResults()

		}

		if j.Status == Completed {

			tmpJob := j
			finishedJobs[j.ID] = tmpJob

			bar.Add(1)
			wg.Done()

		} else {
			//if job isn't completed, send it back to the channel
			pendingJobs <- j
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

	var finishedJobs = make([]Job, len(seqs))
	writer := uilive.New()
	writer.Start()

	pendingJobs := make(chan Job)

	bar := progressbar.Default(int64(len(seqs)))

	// Concurrent job submission in high volume makes the server 😠
	// for w := 1; w <= 3; w++ {
	// 	go jobSubmitter(newJobs, pendingJobs)
	// }

	// Spawn the workers who check if a job has finished running and grabs the results
	// These workers work in the background as new jobs are continuously (slowly) created
	for w := 1; w <= numWorkers; w++ {
		go resultsFetcher(pendingJobs, finishedJobs, bar)
	}

	for i, s := range seqs {
		j := Job{
			ID:         i,
			Status:     Ready,
			Sequence:   s,
			httpClient: Client,
		}
		j.submit()
		pendingJobs <- j
		time.Sleep(time.Second * 5)
		wg.Add(1)
	}

	// fmt.Printf("Created %v sequences.\n", len(seqs))

	wg.Wait()

	elapsed := time.Since(tick)

	f, err := os.Create("data.txt")
	if err != nil {
		panic(err)
	}
	for idx, j := range finishedJobs {
		s := fmt.Sprintf("%v %v\n", idx, j.Results.rna)
		_, err2 := f.WriteString(s)
		if err2 != nil {
			panic(err2)
		}

	}

	fmt.Printf("Completed %v jobs in %s\n", len(finishedJobs), elapsed)

	// fmt.Println(finishedJobs)

}

//TODO: Refactor folder structures
//TODO: Add flags & single sequence search
