package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dadosjusbr/storage"
	"github.com/kelseyhightower/envconfig"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mgoConnTimeout = 60 * time.Second
)

// O tipo decInt é necessário pois a biblioteca converte usando ParseInt passando
// zero na base. Ou seja, meses como 08 passam a ser inválidos pois são tratados
// como números octais.
type decInt int

func (i *decInt) Decode(value string) error {
	v, err := strconv.Atoi(value)
	*i = decInt(v)
	return err
}

type config struct {
	Month decInt `envconfig:"MONTH"`
	Year  decInt `envconfig:"YEAR"`
	AID   string `envconfig:"AID"`

	// Backup URL store
	MongoURI        string `envconfig:"MONGODB_URI"  required:"true"`
	MongoDBName     string `envconfig:"MONGODB_DBNAME"  required:"true"`
	MongoBackupColl string `envconfig:"MONGODB_BCOLL"  required:"true"`
	OutDir          string `envconfig:"OUTPUT_FOLDER"  required:"true"`
}

func main() {
	// parsing environment variables.
	var conf config
	if err := envconfig.Process("", &conf); err != nil {
		log.Fatalf("Error loading config values from .env: %v", err)
	}
	conf.AID = strings.ToLower(conf.AID)

	// configuring mongodb and cloud backup clients.
	db, err := connect(conf.MongoURI)
	if err != nil {
		log.Fatalf("Error connecting to mongo: %v", err)
	}
	defer disconnect(db)
	dbColl := db.Database(conf.MongoDBName).Collection(conf.MongoBackupColl)

	if err := os.MkdirAll(conf.OutDir, os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Error creating output folder(%s): %q", conf.OutDir, err)
	}

	// we are only interested in one field.
	var item struct {
		Backups []storage.Backup `json:"backups,omitempty" bson:"backups,omitempty"`
	}
	err = dbColl.FindOne(
		context.TODO(),
		bson.D{
			{Key: "aid", Value: conf.AID},
			{Key: "year", Value: conf.Year},
			{Key: "month", Value: conf.Month},
		}).Decode(&item)
	if err != nil {
		log.Fatalf("Error searching for agency id \"%s\":%q", conf.AID, err)
	}

	downloads, err := savePackage(item.Backups, conf.OutDir)
	if err != nil {
		log.Fatalf("Error saving backups (%v): %q", item.Backups, err)
	}
	fmt.Println(strings.Join(downloads, "\n"))
}

func connect(url string) (*mongo.Client, error) {
	c, err := mongo.NewClient(options.Client().ApplyURI(url))
	if err != nil {
		return nil, fmt.Errorf("error creating mongo client(%s):%w", url, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), mgoConnTimeout)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to mongo(%s):%w", url, err)
	}
	return c, nil
}

func disconnect(c *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Disconnect(ctx); err != nil {
		return fmt.Errorf("error disconnecting from mongo:%w", err)
	}
	return nil
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
