package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	bucket      = flag.String("bucket", "", "S3 bucket to download files from")
	prefix      = flag.String("prefix", "", "Prefix used to filter which files to download")
	concurrency = flag.Int("concurrency", 100, "The number of concurrent execution")
)

func main() {
	flag.Parse()

	if *bucket == "" {
		fmt.Fprintf(os.Stderr, "bucket needs to be specified\n")
		os.Exit(1)
	}

	fmt.Printf("concurrency: %d\n", *concurrency)
	fmt.Println()

	// Load the Shared AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 Client
	client := s3.NewFromConfig(cfg)

	ch := make(chan string)
	wg := &sync.WaitGroup{}

	for t := 0; t < *concurrency; t++ {
		go download(ch, wg, client, *bucket)
	}

	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(*bucket), Prefix: aws.String(*prefix)})

	for p.HasMorePages() {
		// next page takes a context
		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatal(fmt.Errorf("failed to get a page, %w", err))
		}

		for _, obj := range page.Contents {
			ch <- *obj.Key
		}
	}

	close(ch)
	wg.Wait()
}

func download(ch chan string, wg *sync.WaitGroup, client *s3.Client, bucket string) {
	for key := range ch {
		wg.Add(1)
		defer wg.Done()

		fmt.Printf("downloading %s\n", key)

		resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			log.Fatal(fmt.Errorf("failed to get body, %w", err))
		}
		defer resp.Body.Close()

		dir := path.Dir(key)
		if err = os.MkdirAll(dir, 0766); err != nil {
			panic(err)
		}

		// File
		file, err := os.Create(key)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to get file, %w", err))
		}

		buf := make([]byte, 1024)
		for {
			// read a chunk
			n, err := resp.Body.Read(buf)
			if err != nil && err != io.EOF {
				panic(err)
			}

			if n == 0 {
				break
			}

			// write a chunk
			if _, err = file.Write(buf[:n]); err != nil {
				panic(err)
			}
		}

		// can't use defer because it can cause too many opened files error
		file.Close()
	}
}
