package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const rfamSequenceSearchEndpoint = "https://rfam.org/search/sequence"

type Job struct {
	JobId         string `json:"jobId"`
	Opened        string `json:"opened"`
	EstimatedTime string `json:"estimatedTime"`
	ResultURL     string `json:"resultURL"`
}

type Results struct {
}

type RfamClient struct {
	host       string
	httpClient *http.Client
}

func (c *RfamClient) submit(seq string) Job {

	data := url.Values{}
	data.Set("seq", seq)

	r, _ := http.NewRequest("POST", rfamSequenceSearchEndpoint, strings.NewReader(data.Encode()))

	r.Header.Add("Expect", "")
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Accept", "application/json")

	res, err := c.httpClient.Do(r)

	if err != nil {
		panic(err)
	}

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	var job Job
	err2 := json.Unmarshal(body, &job)
	if err2 != nil {
		panic(err2)
	}

	return job
}

// func (j *Job) getResults() Results

func main() {

	defaultTimeout := time.Second * 10
	rfam := RfamClient{rfamSequenceSearchEndpoint, &http.Client{Timeout: defaultTimeout}}
	job := rfam.submit("CGGGAATAGCTCAGTTGGCTAGAGCATCAGCCTTCCAAGCTGAGGGTCGCGGGTTCGAGCCCCGTTTCCCGCTC")

	fmt.Println(job)

}
