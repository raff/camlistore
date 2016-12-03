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

package s3aws

import (
	"fmt"

	"camlistore.org/pkg/blob"

	"go4.org/syncutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

var statGate = syncutil.NewGate(20) // arbitrary

func (sto *s3Storage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) (err error) {
	if faultStat.FailErr(&err) {
		return
	}
	// TODO: use sto.cache
	var wg syncutil.Group
	for _, br := range blobs {
		br := br
		statGate.Start()
		wg.Go(func() error {
			defer statGate.Done()

			params := &s3.HeadObjectInput{
				Bucket: aws.String(sto.bucket),
				Key:    aws.String(sto.dirPrefix + br.String()),
			}

			res, err := sto.s3Client.HeadObject(params)
			if err == nil {
				dest <- blob.SizedRef{Ref: br, Size: uint32(*res.ContentLength)}
				return nil
			}
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return nil
				}
			}

			return fmt.Errorf("error statting %v: %v", br, err.Error())
		})
	}
	return wg.Err()
}
