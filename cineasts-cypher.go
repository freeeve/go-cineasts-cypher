package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var apikey = flag.String("apikey", "..", "apikey from themoviedb.org")
var delayFlag = flag.Int("delay", 350, "delay between requests, to avoid rate limit blocks")
var delay, _ = time.ParseDuration(fmt.Sprintf("%dms", *delayFlag))

type DiscoverPage struct {
	Page         int64       `json:"page"`
	Results      []MovieType `json:"results"`
	TotalPages   int64       `json:"total_pages"`
	TotalResults int64       `json:"total_results"`
}

type MovieType struct {
	Id          int64       `json:"id"`
	Title       string      `json:"title"`
	Tagline     string      `json:"tagline,omitempty"`
	ReleaseDate string      `json:"release_date"`
	Genres      []GenreType `json:"genres"`
	Casts       CastsType   `json:"casts"`
}

type GenreType struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type CastsType struct {
	Cast []CastType `json:"cast"`
	Crew []CrewType `json:"crew"`
}

type CastType struct {
	Id        int64  `json:"id"`
	Name      string `json:"name"`
	Character string `json:"character"`
}

type CrewType struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
	Job  string `json:"job"`
}

type PersonType struct {
	Id       int64  `json:"id"`
	Name     string `json:"name"`
	Birthday string `json:"birthday"`
	Deathday string `json:"deathday"`
}

func quotes(s string) string {
	re := regexp.MustCompile("\"")
	return "\"" + re.ReplaceAllString(s, "\\\"") + "\""
}

func safe(s string) string {
	re := regexp.MustCompile("[^a-zA-Z]")
	return re.ReplaceAllString(s, "_")
}

func (m MovieType) printMovieCypher() {
	if len(m.ReleaseDate) < 4 {
		return
	}
	release, err := strconv.Atoi(m.ReleaseDate[0:4])
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("MERGE (movie:Movie {id:%d, title:%s, release:%d, tagline:%s})\n",
		m.Id, quotes(m.Title), release, quotes(m.Tagline))
	for _, a := range m.Casts.Cast {
		actor := getPerson(a.Id)
		if len(actor.Name) > 0 && len(actor.Birthday) > 4 && len(strings.Trim(safe(a.Name), "_")) > 0 {
			born, _ := strconv.Atoi(actor.Birthday[0:4])
			fmt.Printf("  MERGE (%s:Person {id:%d, name:%s, born:%d})\n",
				safe(actor.Name), actor.Id, quotes(actor.Name), born)
			fmt.Printf("  SET %s:Actor\n", safe(actor.Name))
			chars2 := make([]string, 0)
			for _, c := range strings.Split(a.Character, "/") {
				chars2 = append(chars2, quotes(strings.TrimSpace(c)))
			}
			chars := strings.Join(chars2, ",")
			fmt.Printf("  CREATE UNIQUE %s-[:ACTED_IN {roles:[%s]}]->movie\n", safe(actor.Name), chars)
		}
	}
	for _, d := range m.Casts.Crew {
		if d.Job == "Director" {
			director := getPerson(d.Id)
			if len(director.Name) > 0 && len(director.Birthday) > 4 && len(strings.Trim(safe(d.Name), "_")) > 0 {
				born, _ := strconv.Atoi(director.Birthday[0:4])
				found := false
				for _, a := range m.Casts.Cast {
					if a.Id == d.Id {
						found = true
					}
				}
				if !found {
					fmt.Printf("  MERGE (%s:Person {id:%d, name:\"%s\", born:%d})\n",
						safe(director.Name), director.Id, director.Name, born)
				}
				fmt.Printf("  SET %s:Director\n", safe(director.Name))
				fmt.Printf("  CREATE UNIQUE %s-[:DIRECTED]->movie\n", safe(director.Name))
			}
		}
	}
	fmt.Println(";")
}

func getPerson(m int64) PersonType {
	time.Sleep(delay)
	client := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("http://api.themoviedb.org/3/person/%d?api_key=%s", m, *apikey), nil)
	req.Header.Add("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	var person PersonType
	json.Unmarshal(body, &person)
	if err != nil {
		log.Fatal(err)
	}
	return person
}

func getMovie(m int64) MovieType {
	time.Sleep(delay)
	client := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("http://api.themoviedb.org/3/movie/%d?api_key=%s&append_to_response=casts", m, *apikey), nil)
	req.Header.Add("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	var movie MovieType
	json.Unmarshal(body, &movie)
	if err != nil {
		log.Fatal(err)
	}
	return movie
}

func discoverMovies(pageNum int) {
	time.Sleep(delay)
	client := &http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("http://api.themoviedb.org/3/discover/movie?page=%d&api_key=%s&append_to_response=casts", pageNum, *apikey), nil)
	req.Header.Add("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	var page DiscoverPage
	json.Unmarshal(body, &page)
	if err != nil {
		log.Fatal(err)
	}
	for _, movie := range page.Results {
		m := getMovie(movie.Id)
		if len(m.Casts.Cast) > 0 && len(m.Casts.Crew) > 0 {
			m.printMovieCypher()
		}
	}

}

func main() {
	flag.Parse()
   if strings.EqualFold(*apikey, "..") {
     fmt.Println("you must specify an API key")
     flag.PrintDefaults()
     return
   }
	for i := 1; i < 6712; i++ {
		discoverMovies(i)
	}
}
