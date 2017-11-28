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
	"strings"
	"path/filepath"
	"log"
)

const (
	DefaultAWSRegion = "cn-north-1"
)

var (
	// 路径清洗正则
	prefixRegexp = regexp.MustCompile(`^[a-zA-Z0-9_]`)
	dotRegexp    = regexp.MustCompile(`\.{2,}`)
	slashRegexp  = regexp.MustCompile(`/{2,}`)

	// 受保护的本地目录,下载的文件不能写入以下目录下
	protectDirectroy = []string{
		"/usr/bin",
		"/usr/etc",
		"/usr/lib",
		"/usr/lib64",
		"/usr/libexec",
		"/usr/sbin",
		"/bin",
		"/sbin",
		"/boot",
		"/etc",
		"/lib",
		"/lib64",
		"/selinux",
		"/sys",
	}
	s3reg  = regexp.MustCompile(`s3://([^\/]+)/(.*)$`)
	input  string
	output string
	region string
)

func init() {
	// s3 路径
	flag.StringVar(&input, "i", "", "s3 path")
	flag.StringVar(&input, "input", "", "s3 path")

	// 本地路径必须是以 / 开头,要求是绝对路径
	// 本地路径,如果最后不是以 / 结尾,则自动补一个 /
	// 本地路径,如果本地路径以 / 结尾,则下载下来到文件名是 /<local path>/<s3 path without input value>
	// 例如输入(input) 是 s3://public/transact/ 该路径下分别有 logs/1.log logs/2017/2.log
	// 设本地路径(/tmp/) 则完整的本地存储路径分别是
	// /tmp/logs/1.log
	// /tmp/logs/2017/2.log
	flag.StringVar(&output, "o", "", "local path")
	flag.StringVar(&output, "output", "", "local path")

	// s3 所在 region
	flag.StringVar(&region, "region", DefaultAWSRegion, "aws region")
	flag.StringVar(&region, "r", DefaultAWSRegion, "aws region")
	flag.Parse()

	if len(input) == 0 || len(output) == 0 {
		flag.Usage()
		os.Exit(-1)
	}
}

// 获取 aws session
func awsSession() *session.Session {
	p := session.Must(session.NewSession(aws.NewConfig().WithRegion(region)))
	cfgs := &aws.Config{Endpoint: aws.String("http://169.254.169.254/latest")}
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvProvider{},
			&ec2rolecreds.EC2RoleProvider{
				Client: ec2metadata.New(p, cfgs),
			},
		})
	config := aws.NewConfig().
		WithRegion(region).
		WithCredentials(creds).
		WithCredentialsChainVerboseErrors(true).
		WithMaxRetries(3)

	return session.Must(session.NewSession(config))
}

// 把完整的 s3 路径分解成 bucket 和 path(prefix)
func parsePath(s3path string) (bucket, path *string) {
	m := s3reg.FindStringSubmatch(s3path)
	if len(m) == 3 {
		return aws.String(m[1]), aws.String(m[2])
	}
	panic(errors.New("s3 path format error: " + s3path))
}

// listObjects 列出指定路径下所有的文件
func listObjects(s3path string) ([]string, error) {
	var rs []string
	bucket, path := parsePath(s3path)
	st := s3.New(awsSession())
	func() {
		var continuationToken *string
	moreObjects:
		in := &s3.ListObjectsV2Input{
			Bucket:            bucket,
			Prefix:            path,
			ContinuationToken: continuationToken,
		}
		st.ListObjectsV2Pages(in, func(out *s3.ListObjectsV2Output, lastPage bool) bool {
			for idx := range out.Contents {
				rs = append(rs, aws.StringValue(out.Contents[idx].Key))
			}
			continuationToken = out.NextContinuationToken
			return lastPage
		})
		if continuationToken != nil {
			goto moreObjects
		}
	}()
	return rs, nil
}

// getObject 从 s3 获取一个对象
func getObject(s3path string) ([]byte, error) {
	bucket, path := parsePath(s3path)

	in := &s3.GetObjectInput{
		Bucket: bucket,
		Key:    path,
	}

	st := s3.New(awsSession())
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

// saveObject 在本地保存文件
func saveObject(content []byte, localpath string) error {
	if err := checkSafePath(localpath); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(localpath), 0755); err != nil {
		return err
	}
	f, err := os.Create(localpath)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write(content)
	return nil
}

// checkSafePath 检查要写入的本地路径是否是安全路径
func checkSafePath(p string) error {
	p, _ = filepath.Abs(p)
	for idx := range protectDirectroy {
		if strings.HasPrefix(p, protectDirectroy[idx]) {
			return errors.New("unsafe path: " + p)
		}
	}
	return nil
}

// 做路径清洗
func cleanPrefixPath(path string) string {
	isAbsPath := strings.HasPrefix(path, "/")
	path = dotRegexp.ReplaceAllString(path, ".")
	path = slashRegexp.ReplaceAllString(path, "/")
	for !prefixRegexp.MatchString(path) {
		path = path[1:]
	}
	if isAbsPath {
		path = fmt.Sprintf("/%s", path)
	}
	return strings.Replace(path, "s3:/", "s3://", 1)
}

// 复制 s3 指定路径下的所有文件到本地
func main() {
	objs, err := listObjects(input)
	if err != nil {
		panic(err.Error())
	}

	bucket, _ := parsePath(input)

	for idx := range objs {
		s := objs[idx]
		if strings.HasSuffix(s, "/") {
			continue
		}
		spath := cleanPrefixPath(fmt.Sprintf("s3://%s/%s", aws.StringValue(bucket), s))
		lpath, err := filepath.Abs(fmt.Sprintf("%s/%s", output, s))
		if err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			panic(err.Error())
		}

		log.Printf("save %s to %s\n", spath, lpath)

		b, err := getObject(spath)
		if err != nil {
			panic(err.Error())
		}
		if err := saveObject(b, cleanPrefixPath(lpath)); err != nil {
			panic(err.Error())
		}
	}
}
