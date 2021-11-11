package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dadosjusbr/storage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI       string `envconfig:"MONGODB_URI" required:"true"`
	MongoDBName    string `envconfig:"MONGODB_NAME" required:"true"`
	MongoMICol     string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol     string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol    string `envconfig:"MONGODB_PKGCOL" required:"true"`
	SwiftUsername  string `envconfig:"SWIFT_USERNAME" required:"true"`
	SwiftAPIKey    string `envconfig:"SWIFT_APIKEY" required:"true"`
	SwiftAuthURL   string `envconfig:"SWIFT_AUTHURL" required:"true"`
	SwiftDomain    string `envconfig:"SWIFT_DOMAIN" required:"true"`
	SwiftContainer string `envconfig:"SWIFT_CONTAINER" required:"true"`
}

var conf config
var client *storage.Client

func newClient(c config) (*storage.Client, error) {
	if c.MongoMICol == "" || c.MongoAgCol == "" {
		return nil, fmt.Errorf("error creating storage client: db collections must not be empty. MI:\"%s\", AG:\"%s\"", c.MongoMICol, c.MongoAgCol)
	}
	db, err := storage.NewDBClient(c.MongoURI, c.MongoDBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(c.MongoMICol)
	bc := storage.NewCloudClient(conf.SwiftUsername, conf.SwiftAPIKey, conf.SwiftAuthURL, conf.SwiftDomain, conf.SwiftContainer)
	client, err := storage.NewClient(db, bc)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}

func main() {
	godotenv.Load()
	if err := envconfig.Process("dadosjusbr-recoletor", &conf); err != nil {
		log.Fatal(err)
	}
	c, err := newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	client = c
	if _, err := strconv.Atoi(os.Getenv("MONTH")); err != nil {
		log.Fatalf("Invalid month (\"%s\"): %q", os.Getenv("MONTH"), err)
	}
	month, _ := strconv.Atoi(os.Getenv("MONTH"))

	if _, err := strconv.Atoi(os.Getenv("YEAR")); err != nil {
		log.Fatalf("Invalid year (\"%s\"): %q", os.Getenv("YEAR"), err)
	}
	year, _ := strconv.Atoi(os.Getenv("YEAR"))

	outputFolder := os.Getenv("OUTPUT_FOLDER")
	if outputFolder == "" {
		outputFolder = "./output"
	}

	if err := os.Mkdir(outputFolder, os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Error creating output folder(%s): %q", outputFolder, err)
	}

	court := strings.ToLower(os.Getenv("COURT"))
	if court == "" {
		log.Fatalf("Environment variable COURT is mandatory")
	}
	downloads, err := savePackage(year, month, court, outputFolder)
	if err != nil {
		log.Fatalf("error while collecting data: %q", err)
	}
	fmt.Println(strings.Join(downloads, "\n"))
}

func savePackage(year, month int, agency string, outDir string) ([]string, error) {
	oma, _, err := client.GetOMA(month, year, agency)
	if err != nil {
		return nil, fmt.Errorf("error fetching data: %v", err)
	}
	var files []string
	for _, file := range oma.Backups {
		if err := download(file.Hash, file.URL); err != nil {
			return nil, fmt.Errorf("error while dowloading file %s: %q", file, err)
		}
		files = append(files, file.URL)
	}
	return files, nil
}

func download(fp string, url string) error {
	os.MkdirAll(filepath.Dir(fp), 0700)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(fp)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}
