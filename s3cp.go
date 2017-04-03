// 从 s3 读取文件,并将文件存储到本地目录

package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	AWSRegion = "cn-north-1"
)

var (
	s3reg  = regexp.MustCompile(`s3://([^\/]+)/(.*)$`)
	input  string
	output string
)

func init() {
	//flag.IntVar(&input,  "input",  value int, usage string)
	flag.StringVar(&input, "i", "", "s3 path")
	flag.StringVar(&input, "input", "", "s3 path")
	flag.StringVar(&output, "o", "", "local path")
	flag.StringVar(&output, "output", "", "local path")
	flag.Parse()
}

func main() {
	b, err := read(input)
	if err != nil {
		panic(err.Error())
	}
	f, err := os.Create(output)
	if err != nil {
		panic(err.Error())
	}
	defer f.Close()
	f.Write(b)
	fmt.Fprintf(os.Stdout, "download %s to %s\n", input, output)
}

func read(s3path string) ([]byte, error) {
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(session.New(aws.NewConfig().WithRegion(AWSRegion)), &aws.Config{Endpoint: aws.String("http://169.254.169.254/latest")}),
			},
		})
	config := aws.NewConfig().
		WithRegion(AWSRegion).
		WithCredentials(creds).
		WithCredentialsChainVerboseErrors(true).
		WithMaxRetries(3)

	sess := session.New(config)

	// 从 s3://public/path/1.doc 这样的路径中分解 bucket 和 path
	bucket, path := func() (string, string) {
		m := s3reg.FindStringSubmatch(s3path)
		if len(m) == 3 {
			return m[1], m[2]
		}
		panic(errors.New("s3 path format error: " + s3path))
	}()

	in := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	}

	st := s3.New(sess)
	out, err := st.GetObject(in)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return b, nil
}
