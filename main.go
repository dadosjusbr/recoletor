package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dadosjusbr/storage"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type config struct {
	MongoURI    string `envconfig:"MONGODB_URI" required:"true"`
	MongoDBName string `envconfig:"MONGODB_NAME" required:"true"`
	MongoMICol  string `envconfig:"MONGODB_MICOL" required:"true"`
	MongoAgCol  string `envconfig:"MONGODB_AGCOL" required:"true"`
	MongoPkgCol string `envconfig:"MONGODB_PKGCOL" required:"true"`
	Month       string `envconfig:"MONTH" required:"true"`
	Year        string `envconfig:"YEAR" required:"true"`
	OutDir      string `envconfig:"OUTPUT_FOLDER"`
	Agency      string `envconfig:"COURT" required:"true"`
}

func newClient(c config) (*storage.Client, error) {
	if c.MongoMICol == "" || c.MongoAgCol == "" {
		return nil, fmt.Errorf("error creating storage client: db collections must not be empty. MI:\"%s\", AG:\"%s\"", c.MongoMICol, c.MongoAgCol)
	}
	db, err := storage.NewDBClient(c.MongoURI, c.MongoDBName, c.MongoMICol, c.MongoAgCol, c.MongoPkgCol)
	if err != nil {
		return nil, fmt.Errorf("error creating DB client: %q", err)
	}
	db.Collection(c.MongoMICol)
	client, err := storage.NewClient(db, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating storage.client: %q", err)
	}
	return client, nil
}

func main() {
	var conf config
	godotenv.Load()
	if err := envconfig.Process("remuneracao-magistrados", &conf); err != nil {
		log.Fatal(err)
	}
	client, err := newClient(conf)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := strconv.Atoi(conf.Month); err != nil {
		log.Fatalf("Invalid month (\"%s\"): %q", conf.Month, err)
	}
	month, _ := strconv.Atoi(conf.Month)

	if _, err := strconv.Atoi(conf.Year); err != nil {
		log.Fatalf("Invalid year (\"%s\"): %q", conf.Year, err)
	}
	year, _ := strconv.Atoi(conf.Year)

	outputFolder := conf.OutDir
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
	oma, _, err := client.GetOMA(month, year, court)
	if err != nil {
		log.Fatalf("error fetching data: %q", err)
	}
	downloads, err := savePackage(oma.Backups, outputFolder)
	if err != nil {
		log.Fatalf("error while collecting data: %q", err)
	}
	fmt.Println(strings.Join(downloads, "\n"))
}

func savePackage(backups []storage.Backup, outDir string) ([]string, error) {
	var files []string
	for _, file := range backups {
		if err := download(fmt.Sprintf("%s/%s", outDir, path.Base(file.URL)), file.URL); err != nil {
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
