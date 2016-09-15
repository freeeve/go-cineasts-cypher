package main

import (
	"encoding/csv"
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

// command-line flags and related vars
var apikey = flag.String("apikey", "..", "apikey from themoviedb.org")
var delayFlag = flag.Int("delay", 300, "delay between requests, to avoid rate limit blocks")
var delay time.Duration
var votecount = flag.Int("votecount", 10, "minimum votecount, used to filter out lesser-known films")

// JSON structs--only have the data I care about
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
	VoteAverage float64     `json:"vote_average"`
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

// wrap a string in quotes, and escape the quotes inside
func quotes(s string) string {
	re := regexp.MustCompile("\\\\")
	str := re.ReplaceAllString(s, "\\\\")
	re = regexp.MustCompile("\"")
	str = re.ReplaceAllString(str, "\\\"")
	return "\"" + str + "\""
}

// convert non alpha chars to underscores... for use as identifiers in Cypher
func safe(s string) string {
	return safeWithReplace(s, "_")
}

// convert non alpha chars to something
func safeWithReplace(s string, replace string) string {
	re := regexp.MustCompile("[^a-zA-Z]")
	return re.ReplaceAllString(s, replace)
}

// is an int in a slice
func inList(x int64, l []int64) bool {
	for _, item := range l {
		if item == x {
			return true
		}
	}
	return false
}

// get the born year out of a date string
func getBorn(p PersonType) int {
	if len(p.Birthday) > 4 {
		born, _ := strconv.Atoi(p.Birthday[0:4])
		return born
	}
	return 0
}

// get the died year out of a date string (TODO make it so this is not so redundant to the above)
func getDied(p PersonType) int {
	if len(p.Deathday) > 4 {
		died, _ := strconv.Atoi(p.Deathday[0:4])
		return died
	}
	return 0
}

// strip out slashes so the URL can be saved as a filename
func getFileSafe(s string) string {
	re := regexp.MustCompile(fmt.Sprintf("/|%s", *apikey))
	return re.ReplaceAllString(s, "_")
}

// create a list of character strings like:
// "Big Momma", "Malcolm Turner"
func makeCharsString(char string) string {
	chars := make([]string, 0)
	for _, c := range regexp.MustCompile("/|\\\\").Split(char, 100) {
		chars = append(chars, strings.TrimSpace(c))
	}
	return strings.Join(chars, ":")
}

func (m MovieType) printMovieCSV(w *csv.Writer) {
	if len(m.ReleaseDate) < 4 {
		return
	}
	release, err := strconv.Atoi(m.ReleaseDate[0:4])
	if err != nil {
		log.Println(err)
	}
	genres := []string{}
	for _, genre := range m.Genres {
		genres = append(genres, genre.Name)
	}
	genreStr := strings.Join(genres, ":")
	record := []string{}
	record = append(record, fmt.Sprintf("%d", m.Id))
	record = append(record, m.Title)
	record = append(record, fmt.Sprintf("%f", m.VoteAverage))
	record = append(record, strconv.Itoa(release))
	record = append(record, m.Tagline)
	record = append(record, genreStr)
	err = w.Write(record)
	if err != nil {
		log.Fatal(err)
	}
}

var personMap = map[int64]bool{}

func (m MovieType) printPeopleCSV(w *csv.Writer) {
	if len(m.ReleaseDate) < 4 {
		return
	}
	for _, a := range m.Casts.Cast {
		actor := getPerson(a.Id)
		if len(actor.Name) > 0 && len(actor.Birthday) > 4 && len(strings.Trim(safe(a.Name), "_")) > 0 {
			born := getBorn(actor)
			died := getDied(actor)
			record := []string{}
			record = append(record, fmt.Sprintf("%d", actor.Id))
			record = append(record, actor.Name)
			record = append(record, strconv.Itoa(born))
			record = append(record, strconv.Itoa(died))
			if !personMap[actor.Id] {
				err := w.Write(record)
				if err != nil {
					log.Fatal(err)
				}
				personMap[actor.Id] = true
			}
		}
	}
	for _, d := range m.Casts.Crew {
		if d.Job == "Director" {
			director := getPerson(d.Id)
			born := getBorn(director)
			died := getDied(director)
			record := []string{}
			record = append(record, fmt.Sprintf("%d", director.Id))
			record = append(record, director.Name)
			record = append(record, strconv.Itoa(born))
			record = append(record, strconv.Itoa(died))
			if !personMap[director.Id] {
				err := w.Write(record)
				if err != nil {
					log.Fatal(err)
				}
				personMap[director.Id] = true
			}
		}
	}
}

func (m MovieType) printActorsCSV(w *csv.Writer) {
	if len(m.ReleaseDate) < 4 {
		return
	}
	for _, a := range m.Casts.Cast {
		actor := getPerson(a.Id)
		if len(actor.Name) > 0 && len(actor.Birthday) > 4 && len(strings.Trim(safe(a.Name), "_")) > 0 {
			record := []string{}
			record = append(record, fmt.Sprintf("%d", actor.Id))
			record = append(record, fmt.Sprintf("%d", m.Id))
			record = append(record, makeCharsString(a.Character))
			err := w.Write(record)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (m MovieType) printDirectorsCSV(w *csv.Writer) {
	if len(m.ReleaseDate) < 4 {
		return
	}
	for _, d := range m.Casts.Crew {
		if d.Job == "Director" {
			director := getPerson(d.Id)
			record := []string{}
			record = append(record, fmt.Sprintf("%d", director.Id))
			record = append(record, fmt.Sprintf("%d", m.Id))
			err := w.Write(record)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// get this URL from our cache or call the API and cache the response
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

// the person API call
func getPerson(m int64) PersonType {
	url := fmt.Sprintf("http://api.themoviedb.org/3/person/%d?api_key=%s", m, *apikey)
	body := getCacheOrRequest(url)
	var person PersonType
	json.Unmarshal(body, &person)
	return person
}

// the movie API call
func getMovie(m int64) MovieType {
	url := fmt.Sprintf("http://api.themoviedb.org/3/movie/%d?api_key=%s&append_to_response=casts", m, *apikey)
	body := getCacheOrRequest(url)
	var movie MovieType
	json.Unmarshal(body, &movie)
	return movie
}

// the discover API call (recursive)
func discoverMovies(pageNum int64, moviesC *csv.Writer, peopleC *csv.Writer, actorsC *csv.Writer, directorsC *csv.Writer) {
	url := fmt.Sprintf("http://api.themoviedb.org/3/discover/movie?page=%d&api_key=%s&&vote_count.gte=%d",
		pageNum, *apikey, *votecount)
	body := getCacheOrRequest(url)
	var page DiscoverPage
	json.Unmarshal(body, &page)
	for _, movie := range page.Results {
		m := getMovie(movie.Id)
		if len(m.Casts.Cast) > 0 && len(m.Casts.Crew) > 0 {
			m.printMovieCSV(moviesC)
			m.printPeopleCSV(peopleC)
			m.printActorsCSV(actorsC)
			m.printDirectorsCSV(directorsC)
		}
	}
	if pageNum < page.TotalPages {
		discoverMovies(pageNum+1, moviesC, peopleC, actorsC, directorsC)
	}
}

// create a cache folder and spit out indexes
func main() {
	flag.Parse()
	if strings.EqualFold(*apikey, "..") {
		fmt.Println("you must specify an API key")
		flag.PrintDefaults()
		return
	}
	delay, _ = time.ParseDuration(fmt.Sprintf("%dms", *delayFlag))
	os.Mkdir("cache", 0755)
	os.Chdir("cache")
	f, err := os.Create("movies.csv")
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	moviesC := csv.NewWriter(f)
	record := []string{}
	record = append(record, "movieId")
	record = append(record, "title")
	record = append(record, "avgVote")
	record = append(record, "releaseYear")
	record = append(record, "tagline")
	record = append(record, "genres")
	err = moviesC.Write(record)
	if err != nil {
		log.Fatal(err)
	}
	f2, err := os.Create("people.csv")
	defer f2.Close()
	if err != nil {
		log.Fatal(err)
	}
	peopleC := csv.NewWriter(f2)
	record = []string{}
	record = append(record, "personId")
	record = append(record, "name")
	record = append(record, "birthYear")
	record = append(record, "deathYear")
	err = peopleC.Write(record)
	if err != nil {
		log.Fatal(err)
	}
	f3, err := os.Create("actors.csv")
	defer f3.Close()
	if err != nil {
		log.Fatal(err)
	}
	actorsC := csv.NewWriter(f3)
	record = []string{}
	record = append(record, "personId")
	record = append(record, "movieId")
	record = append(record, "characters")
	err = actorsC.Write(record)
	if err != nil {
		log.Fatal(err)
	}
	f4, err := os.Create("directors.csv")
	defer f4.Close()
	if err != nil {
		log.Fatal(err)
	}
	directorsC := csv.NewWriter(f4)
	record = []string{}
	record = append(record, "personId")
	record = append(record, "movieId")
	err = directorsC.Write(record)
	if err != nil {
		log.Fatal(err)
	}
	discoverMovies(1, moviesC, peopleC, actorsC, directorsC)
	moviesC.Flush()
	peopleC.Flush()
	actorsC.Flush()
	directorsC.Flush()
}
