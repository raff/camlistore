/*
Copyright 2011 Google Inc.

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

/*
Package s3aws registers the "s3aws" blobserver storage type, storing
blobs in an Amazon Web Services' S3 storage bucket.

Example low-level config:

     "/r1/": {
         "handler": "storage-s3aws",
         "handlerArgs": {
            "bucket": "foo",
            "region": "...",
            "aws_access_key": "...",
            "aws_secret_access_key": "...",
            "skipStartupCheck": false
          }
     },

*/
package s3aws // import "camlistore.org/pkg/blobserver/s3aws"

import (
	"fmt"
	"strings"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/memory"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"go4.org/fault"
	"go4.org/jsonconfig"
)

var (
	_ blob.SubFetcher               = (*s3Storage)(nil)
	_ blobserver.MaxEnumerateConfig = (*s3Storage)(nil)
)

var (
	faultReceive   = fault.NewInjector("s3_receive")
	faultEnumerate = fault.NewInjector("s3_enumerate")
	faultStat      = fault.NewInjector("s3_stat")
	faultGet       = fault.NewInjector("s3_get")
)

const maxParallelHTTP = 5

type s3Storage struct {
	s3Client *s3.S3
	bucket   string
	// optional "directory" where the blobs are stored, instead of at the root of the bucket.
	// S3 is actually flat, which in effect just means that all the objects should have this
	// dirPrefix as a prefix of their key.
	// If non empty, it should be a slash separated path with a trailing slash and no starting
	// slash.
	dirPrefix string
	region    string
	cache     *memory.Storage // or nil for no cache
}

func (s *s3Storage) String() string {
	region := s.region
	if region == "" {
		region = "default"
	}
	if s.dirPrefix != "" {
		return fmt.Sprintf("\"s3\" blob storage at region %q, bucket %q, directory %q", region, s.bucket, s.dirPrefix)
	}
	return fmt.Sprintf("\"s3\" blob storage at region %q, bucket %q", region, s.bucket)
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (blobserver.Storage, error) {
	bucket := config.RequiredString("bucket")
	accessKey := config.RequiredString("aws_access_key")
	secret := config.RequiredString("aws_secret_access_key")
	region := config.OptionalString("region", "")
	cacheSize := config.OptionalInt64("cacheSize", 32<<20)
	awsConfig := aws.NewConfig()
	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}
	if accessKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secret, ""))
	}
	var dirPrefix string
	if parts := strings.SplitN(bucket, "/", 2); len(parts) > 1 {
		dirPrefix = parts[1]
		bucket = parts[0]
	}
	if dirPrefix != "" && !strings.HasSuffix(dirPrefix, "/") {
		dirPrefix += "/"
	}
	sto := &s3Storage{
		s3Client:  s3.New(session.New(awsConfig)),
		bucket:    bucket,
		dirPrefix: dirPrefix,
	}
	skipStartupCheck := config.OptionalBool("skipStartupCheck", false)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if cacheSize != 0 {
		sto.cache = memory.NewCache(cacheSize)
	}
	if !skipStartupCheck {
		params := &s3.HeadBucketInput{
			Bucket: aws.String(sto.bucket), // Required
		}

		_, err := sto.s3Client.HeadBucket(params)
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NoSuchBucket" {
				return nil, fmt.Errorf("Bucket %q doesn't exist.", sto.bucket)
			}
		}

		if err != nil {
			return nil, fmt.Errorf("Error validating bucket %s: %v", sto.bucket, err)
		}
	}
	return sto, nil
}

func init() {
	blobserver.RegisterStorageConstructor("s3aws", blobserver.StorageConstructor(newFromConfig))
}
