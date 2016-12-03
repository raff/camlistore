/*
Copyright 2014 The Camlistore Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3aws

import (
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/storagetest"
	"camlistore.org/pkg/schema"

	"go4.org/jsonconfig"
	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	key          = flag.String("s3_key", "", "AWS access Key ID")
	secret       = flag.String("s3_secret", "", "AWS access secret")
	region       = flag.String("s3_region", "", "AWS S3 region")
	bucket       = flag.String("s3_bucket", "", "Bucket name to use for testing. If empty, testing is skipped. If non-empty, it must begin with 'camlistore-' and end in '-test' and have zero items in it.")
	flagTestData = flag.String("testdata", "", "Optional directory containing some files to write to the bucket, for additional tests.")
)

func TestS3(t *testing.T) {
	testStorage(t, "")
}

func TestS3WithBucketDir(t *testing.T) {
	testStorage(t, "/bl/obs/")
}

func jsonConfig(key, secret, region, bucket string) jsonconfig.Obj {
	config := jsonconfig.Obj{"bucket": bucket}

	if key != "" {
		config["aws_access_key"] = key
	}
	if secret != "" {
		config["aws_secret_access_key"] = secret
	}
	if region != "" {
		config["region"] = region
	}

	return config
}

func TestS3WriteFiles(t *testing.T) {
	if *flagTestData == "" {
		t.Skipf("testdata dir not specified, skipping test.")
	}
	sto, err := newFromConfig(nil, jsonConfig(*key, *secret, *region, *bucket))
	if err != nil {
		t.Fatalf("newFromConfig error: %v", err)
	}
	dir, err := os.Open(*flagTestData)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	for _, name := range names {
		f, err := os.Open(filepath.Join(*flagTestData, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() // assuming there aren't that many files.
		if _, err := schema.WriteFileFromReaderWithModTime(sto, name, time.Now(), f); err != nil {
			t.Fatalf("Error while writing %v to S3: %v", name, err)
		}
		t.Logf("Wrote %v successfully to S3", name)
	}
}

func testStorage(t *testing.T, bucketDir string) {
	if *bucket == "" {
		t.Skip("Skipping test because -s3_bucket flags has not been provided.")
	}
	if !strings.HasPrefix(*bucket, "camlistore-") || !strings.HasSuffix(*bucket, "-test") {
		t.Fatalf("bogus bucket name %q; must begin with 'camlistore-' and end in '-test'", *bucket)
	}

	bucketWithDir := path.Join(*bucket, bucketDir)

	storagetest.Test(t, func(t *testing.T) (sto blobserver.Storage, cleanup func()) {
		sto, err := newFromConfig(nil, jsonConfig(*key, *secret, *region, bucketWithDir))
		if err != nil {
			t.Fatalf("newFromConfig error: %v", err)
		}
		if !testing.Short() {
			log.Printf("Warning: this test does many serial operations. Without the go test -short flag, this test will be very slow.")
		}
		if bucketWithDir != *bucket {
			// Adding "a", and "c" objects in the bucket to make sure objects out of the
			// "directory" are not touched and have no influence.
			for _, key := range []string{"a", "c"} {
				var buf bytes.Buffer
				size, err := io.Copy(&buf, strings.NewReader(key))
				if err != nil {
					t.Fatalf("could not insert object %s in bucket %v: %v", key, sto.(*s3Storage).bucket, err)
				}

				params := &s3.PutObjectInput{
					Bucket:        aws.String(sto.(*s3Storage).bucket),
					Key:           aws.String(key),
					Body:          bytes.NewReader(buf.Bytes()),
					ContentLength: aws.Int64(size),
				}

				_, err = sto.(*s3Storage).s3Client.PutObject(params)
				if err != nil {
					t.Fatalf("could not insert object %s in bucket %v: %v", key, sto.(*s3Storage).bucket, err)
				}
			}
		}
		clearBucket := func(beforeTests bool) func() {
			return func() {
				var all []blob.Ref
				blobserver.EnumerateAll(context.TODO(), sto, func(sb blob.SizedRef) error {
					t.Logf("Deleting: %v", sb.Ref)
					all = append(all, sb.Ref)
					return nil
				})
				if err := sto.RemoveBlobs(all); err != nil {
					t.Fatalf("Error removing blobs during cleanup: %v", err)
				}
				if beforeTests {
					return
				}
				if bucketWithDir != *bucket {
					// checking that "a" and "c" at the root were left untouched.
					for _, key := range []string{"a", "c"} {
						getParams := &s3.GetObjectInput{
							Bucket: aws.String(sto.(*s3Storage).bucket),
							Key:    aws.String(key),
						}
						if _, err := sto.(*s3Storage).s3Client.GetObject(getParams); err != nil {
							t.Fatalf("could not find object %s after tests: %v", key, err)
						}

						delParams := &s3.DeleteObjectInput{
							Bucket: aws.String(sto.(*s3Storage).bucket),
							Key:    aws.String(key),
						}
						if _, err := sto.(*s3Storage).s3Client.DeleteObject(delParams); err != nil {
							t.Fatalf("could not remove object %s after tests: %v", key, err)
						}
					}
				}
			}
		}
		clearBucket(true)()
		return sto, clearBucket(false)
	})
}

func TestNextStr(t *testing.T) {
	tests := []struct {
		s, want string
	}{
		{"", ""},
		{"abc", "abd"},
		{"ab\xff", "ac\x00"},
		{"a\xff\xff", "b\x00\x00"},
		{"sha1-da39a3ee5e6b4b0d3255bfef95601890afd80709", "sha1-da39a3ee5e6b4b0d3255bfef95601890afd8070:"},
	}
	for _, tt := range tests {
		if got := nextStr(tt.s); got != tt.want {
			t.Errorf("nextStr(%q) = %q; want %q", tt.s, got, tt.want)
		}
	}
}
