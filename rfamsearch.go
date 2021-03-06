package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gosuri/uiprogress"
	"github.com/mitchellh/colorstring"
	"github.com/mitchellh/mapstructure"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	ID          int         //unique job id, also serves as index for the final output array
	Sequence    DNASequence //sequence to search Rfam for
	Status      State
	LastChecked time.Time //last time the job status was checked via a get request
	JobId       string    `json:"jobId"`
	Opened      string    `json:"opened"`
	ResultURL   string    `json:"resultURL"`
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

// var serverResponses = map[int]string{
// 	202: "Accepted",
// 	201: "Submitted",
// 	502: "Bad gateway",
// 	503: "Service unavailable",
// 	200: "OK",
// 	410: "Gone",
// 	510: "Service unvailable",
// 	500: "Internal server error",
// }

//Submit a sequence to Rfam
func (j *Job) submit(httpClient *http.Client) {

	if !(j.Status == Ready) {
		panic("Job not Ready")
	}

	data := url.Values{}
	data.Set("seq", j.Sequence.seq)

	r, _ := http.NewRequest(http.MethodPost, rfamSequenceSearchEndpoint, strings.NewReader(data.Encode()))
	r.Close = true
	r.Header.Add("Expect", "")
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Accept", "application/json")

	res, err := httpClient.Do(r)

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

//Check if Job has finished running and grab the results
func (j *Job) getResults(httpClient *http.Client) {

	if !(j.Status == Submitted || j.Status == Pending) {
		panic(fmt.Sprintf("Unsubmitted Job(%v) %v", j.ID, j.Status))
	}

	r, err := http.NewRequest(http.MethodGet, j.ResultURL, strings.NewReader(url.Values{}.Encode()))
	r.Close = true

	if err != nil {
		panic(err)
	}

	r.Header.Add("Expect", "")
	r.Header.Add("Accept", "application/json")

	res, getErr := httpClient.Do(r)
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

	if results.closed != "" && j.ResultURL != "" {
		j.Status = Completed
	} else {
		j.Status = Submitted
	}

}

var wg sync.WaitGroup

func jobSubmitter(newJobs chan Job, pendingJobs chan<- Job, client *http.Client, bar *uiprogress.Bar) {
	for j := range newJobs {
		j.submit(client)

		if j.ResultURL == "" {
			j.Status = Ready
			newJobs <- j

		} else {
			pendingJobs <- j
			bar.Incr()
		}
	}
}

func resultsFetcher(pendingJobs chan Job, finishedJobs []Job, client *http.Client, bar *uiprogress.Bar) {
	for j := range pendingJobs {

		if time.Since(j.LastChecked) > time.Second*5 {
			j.getResults(client)
		}

		if j.Status == Completed {
			tmpJob := j
			finishedJobs[j.ID] = tmpJob
			bar.Incr()
			wg.Done()

		} else {
			//if job isn't completed, send it back to the channel
			pendingJobs <- j
		}
	}
}

type DNASequence struct {
	seq    string
	name   string
	length int
}

func (ds *DNASequence) Colorize(autotrim bool) string {
	s := ds.seq
	if autotrim {
		s = ds.TrimDNA()
	}
	replacer := strings.NewReplacer("A", "[red]A", "T", "[blue]T", "G", "[green]G", "C", "[yellow]C", ".....", "[white].....")
	return replacer.Replace(s)
}

func (ds *DNASequence) TrimDNA() string {
	trimmed := fmt.Sprintf("%v ..... %v", ds.seq[:30], ds.seq[:30])
	return trimmed
}

func readFasta(filename string) []DNASequence {
	var seqs []DNASequence

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	data := strings.Split(string(file), ">")

	for _, entry := range data[1:] {
		sq := strings.Split(entry, "\n")
		longSeq := strings.Join(sq[1:], "")
		seqs = append(seqs, DNASequence{
			seq:    longSeq,
			name:   sq[0],
			length: len(longSeq),
		})
	}

	return seqs
}

//save results to file (tab separated)
func saveToFile(outfile string, text string) {
	f, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}

	header := fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\n", "index", "rna", "family", "length", "label", "sequence")

	_, errx := f.WriteString(header)
	if errx != nil {
		panic(errx)
	}

	_, err2 := f.WriteString(text)
	if err2 != nil {
		panic(err2)
	}
}

func main() {

	tick := time.Now()

	var filename string
	var numWorkers int
	var output string
	var seq string
	var submitters int

	flag.IntVar(&numWorkers, "n", 10, "Number of workers monitoring running jobs")
	flag.StringVar(&filename, "f", "", "Fasta file")
	flag.StringVar(&output, "o", "data.txt", "Output file")
	flag.StringVar(&seq, "seq", "", "Single DNA sequence")
	flag.Parse()

	if filename == "" && seq == "" {
		fmt.Println("No input files provided.")
		os.Exit(3)
	}

	//initialize shared http client
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	postClient := &http.Client{
		Timeout:   time.Second * 200,
		Transport: t,
	}

	getClient := &http.Client{
		Timeout:   time.Second * 200,
		Transport: t,
	}

	var seqs []DNASequence
	submitters = 10

	if seq != "" {
		ds := DNASequence{
			seq:    seq,
			name:   "",
			length: len(seq),
		}

		seqs = append(seqs, ds)
		wg.Add(1)
		submitters = 1
		numWorkers = 1

	} else {
		//read fasta file with input sequences
		seqs = readFasta(filename)
		wg.Add(len(seqs))

	}

	finishedJobs := make([]Job, len(seqs))
	pendingJobs := make(chan Job, len(seqs))
	newJobs := make(chan Job, len(seqs))

	//live feedbacks via multiple progressbars
	jobsBar := uiprogress.AddBar(len(seqs)).AppendCompleted().PrependElapsed()
	submittedBar := uiprogress.AddBar(len(seqs)).AppendCompleted().PrependElapsed()
	completedBar := uiprogress.AddBar(len(seqs)).AppendCompleted().PrependElapsed()

	jobsBar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("???? Created   %d/%d jobs", b.Current(), len(seqs))
	})

	submittedBar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("???? Submitted %d/%d jobs", b.Current(), len(seqs))
	})

	completedBar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("???? Completed %d/%d jobs", b.Current(), len(seqs))
	})

	uiprogress.Start()

	// Submits new search jobs, moving them from the new jobs queue to the pending queue
	for w := 1; w <= submitters; w++ {
		go jobSubmitter(newJobs, pendingJobs, getClient, submittedBar)
	}

	// Spawn the workers who check if a job has finished running and grabs the results
	// These workers work in the background as new jobs are continuously (slowly) created
	for w := 1; w <= numWorkers; w++ {
		go resultsFetcher(pendingJobs, finishedJobs, postClient, completedBar)
	}

	//jobs are created in small batches to avoid overwhelming the rfam server
	batchSize := 10

	for i := 0; i < len(seqs); i += batchSize {
		k := i + batchSize
		if k > len(seqs) {
			k = len(seqs)
		}

		for idx, s := range seqs[i:k] {
			j := Job{
				ID:       i + idx,
				Status:   Ready,
				Sequence: s,
			}
			newJobs <- j
			jobsBar.Incr()
		}
		time.Sleep(time.Second * 5)
	}

	wg.Wait() //wg.Done() is called by resultsFetchers
	uiprogress.Stop()

	fmt.Println("")

	elapsed := time.Since(tick)

	var s string
	for idx, j := range finishedJobs {
		rna := j.Results.rna
		var fam string
		if rna == "" {
			rna = "NoMatch"
		} else {
			fam = j.Results.rnaMatch[0].Acc
		}
		s += fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\n", idx, rna, fam, j.Sequence.length, j.Sequence.name, j.Sequence.seq)
		s += "\n"
		if j.Results.rna != "" {
			colorstring.Print(j.Sequence.Colorize(true))
			fmt.Print("\t", j.Results.rna)
			fmt.Println("\t", j.Results.rnaMatch[0].Acc)
		}
	}

	if seq == "" {
		saveToFile(output, s)
	}

	fmt.Printf("\nCompleted %v jobs in %s\n", len(finishedJobs), elapsed)

}
