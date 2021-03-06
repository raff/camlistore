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
	"camlistore.org/pkg/blob"

	"go4.org/syncutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

var removeGate = syncutil.NewGate(20) // arbitrary

func (sto *s3Storage) RemoveBlobs(blobs []blob.Ref) error {
	if sto.cache != nil {
		sto.cache.RemoveBlobs(blobs)
	}
	var wg syncutil.Group

	for _, blob := range blobs {
		blob := blob
		removeGate.Start()
		wg.Go(func() error {
			defer removeGate.Done()

			params := &s3.DeleteObjectInput{
				Bucket: aws.String(sto.bucket),
				Key:    aws.String(sto.dirPrefix + blob.String()),
			}

			_, err := sto.s3Client.DeleteObject(params)
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				if reqErr.StatusCode() == 404 {
					return nil
				}
			}
			return err

		})
	}
	return wg.Err()

}
