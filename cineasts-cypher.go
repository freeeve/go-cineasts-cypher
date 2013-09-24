package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var apikey = flag.String("apikey", "..", "apikey from themoviedb.org")
var delayFlag = flag.Int("delay", 350, "delay between requests, to avoid rate limit blocks")
var delay, _ = time.ParseDuration(fmt.Sprintf("%dms", *delayFlag))
var votecount = flag.Int("votecount", 10, "minimum votecount, used to filter out lesser-known films")

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

func safeWithReplace(s string, replace string) string {
	re := regexp.MustCompile("[^a-zA-Z]")
	return re.ReplaceAllString(s, replace)
}

func inList(x int64, l []int64) bool {
	for _, item := range l {
		if item == x {
			return true
		}
	}
	return false
}

func getBorn(p PersonType) int {
	if len(p.Birthday) > 4 {
		born, _ := strconv.Atoi(p.Birthday[0:4])
		return born
	}
	return 0
}

func getDied(p PersonType) int {
	if len(p.Deathday) > 4 {
		died, _ := strconv.Atoi(p.Deathday[0:4])
		return died
	}
	return 0
}

func getFileSafe(s string) string {
	re := regexp.MustCompile(fmt.Sprintf("/|%s", *apikey))
	return re.ReplaceAllString(s, "_")
}

func makeCharsString(char string) string {
	chars := make([]string, 0)
	for _, c := range strings.Split(char, "/") {
		chars = append(chars, quotes(strings.TrimSpace(c)))
	}
	return strings.Join(chars, ",")
}

func (m MovieType) printMovieCypher() {
	if len(m.ReleaseDate) < 4 {
		return
	}
	release, err := strconv.Atoi(m.ReleaseDate[0:4])
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("MERGE (movie:Movie {id:%d})\n", m.Id)
	fmt.Printf("  ON CREATE movie SET movie.title = %s\n", quotes(m.Title))
	fmt.Printf("    , movie.release = %d\n", release)
	if len(m.Tagline) > 0 {
		fmt.Printf("    , movie.tagline = %s\n", quotes(m.Tagline))
	}
	for _, genre := range m.Genres {
		fmt.Printf("    , movie:%s\n", safeWithReplace(genre.Name, ""))
	}
	var actors = make([]int64, 0)
	for _, a := range m.Casts.Cast {
		actor := getPerson(a.Id)
		if len(actor.Name) > 0 && len(actor.Birthday) > 4 && len(strings.Trim(safe(a.Name), "_")) > 0 {
			if !inList(actor.Id, actors) {
				fmt.Printf("  MERGE (%s:Person {id:%d})\n",
					safe(actor.Name), actor.Id)
				born := getBorn(actor)
				died := getDied(actor)
				fmt.Printf("  ON CREATE %s SET %s.name = %s\n",
					safe(actor.Name), safe(actor.Name), quotes(actor.Name))
				if born > 0 {
					fmt.Printf("    , %s.born = %d\n", safe(actor.Name), born)
				}
				if died > 0 {
					fmt.Printf("    , %s.died = %d\n", safe(actor.Name), died)
				}
				fmt.Printf("  SET %s:Actor\n", safe(actor.Name))
				chars := makeCharsString(a.Character)
				fmt.Printf("  CREATE UNIQUE (%s)-[%s_act:ACTED_IN]->(movie)\n",
					safe(actor.Name), safe(actor.Name))
				fmt.Printf("  SET %s_act.roles = [%s]\n", safe(actor.Name), chars)
				actors = append(actors, actor.Id)
			} else {
				chars := makeCharsString(a.Character)
				fmt.Printf("  SET %s_act.roles = filter(x in %s_act.roles where not(x in([%s]))) + [%s]\n",
					safe(actor.Name), safe(actor.Name), chars, chars)
			}
		}
	}
	for _, d := range m.Casts.Crew {
		if d.Job == "Director" {
			director := getPerson(d.Id)
			if len(strings.Trim(safe(d.Name), "_")) > 0 {
				born := getBorn(director)
				died := getDied(director)
				if !inList(d.Id, actors) {
					// TODO make sure safe(director.Name) hasn't been used already
					// some people have the same name acting/directing in the same movie
					fmt.Printf("  MERGE (%s:Person {id:%d})\n", safe(director.Name), director.Id)
					fmt.Printf("  ON CREATE %s SET %s.name = %s\n",
						safe(director.Name), safe(director.Name), quotes(director.Name))
					if born > 0 {
						fmt.Printf("    , %s.born = %d\n", safe(director.Name), born)
					}
					if died > 0 {
						fmt.Printf("    , %s.died = %d\n", safe(director.Name), died)
					}
					actors = append(actors, d.Id)
				}
				fmt.Printf("  SET %s:Director\n", safe(director.Name))
				fmt.Printf("  CREATE UNIQUE (%s)-[:DIRECTED]->(movie)\n", safe(director.Name))
			}
		}
	}
	fmt.Println(";")
}

func getCacheOrRequest(url string) []byte {
	body, err := ioutil.ReadFile(getFileSafe(url))
	if err != nil {
		time.Sleep(delay)
		client := &http.Client{}
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Add("Accept", "application/json")
		res, _ := client.Do(req)
		body, _ = ioutil.ReadAll(res.Body)
		res.Body.Close()
		ioutil.WriteFile(getFileSafe(url), body, 0644)
	}
   return body
}

func getPerson(m int64) PersonType {
	url := fmt.Sprintf("http://api.themoviedb.org/3/person/%d?api_key=%s", m, *apikey)
	body := getCacheOrRequest(url)
	var person PersonType
	json.Unmarshal(body, &person)
	return person
}

func getMovie(m int64) MovieType {
	url := fmt.Sprintf("http://api.themoviedb.org/3/movie/%d?api_key=%s&append_to_response=casts", m, *apikey)
	body := getCacheOrRequest(url)
	var movie MovieType
	json.Unmarshal(body, &movie)
	return movie
}

func discoverMovies(pageNum int64) {
	url := fmt.Sprintf("http://api.themoviedb.org/3/discover/movie?page=%d&api_key=%s&&vote_count.gte=%d",
		pageNum, *apikey, *votecount)
	body := getCacheOrRequest(url)
	var page DiscoverPage
	json.Unmarshal(body, &page)
	for _, movie := range page.Results {
		m := getMovie(movie.Id)
		if len(m.Casts.Cast) > 0 && len(m.Casts.Crew) > 0 {
			m.printMovieCypher()
		}
	}
	if pageNum < page.TotalPages {
		discoverMovies(pageNum + 1)
	}
}

func main() {
	flag.Parse()
	if strings.EqualFold(*apikey, "..") {
		fmt.Println("you must specify an API key")
		flag.PrintDefaults()
		return
	}
	os.Mkdir("cache", 0755)
	os.Chdir("cache")
	fmt.Println("CREATE INDEX on :Movie(id);")
	fmt.Println("CREATE INDEX on :Movie(title);")
	fmt.Println("CREATE INDEX on :Person(id);")
	fmt.Println("CREATE INDEX on :Person(name);")
	discoverMovies(1)
}
