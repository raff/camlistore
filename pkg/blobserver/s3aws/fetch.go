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
	"io"

	"camlistore.org/pkg/blob"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (sto *s3Storage) Fetch(blob blob.Ref) (file io.ReadCloser, size uint32, err error) {
	if faultGet.FailErr(&err) {
		return
	}
	if sto.cache != nil {
		if file, size, err = sto.cache.Fetch(blob); err == nil {
			return
		}
	}

	params := &s3.GetObjectInput{
		Bucket: aws.String(sto.bucket),
		Key:    aws.String(sto.dirPrefix + blob.String()),
	}

	resp, err := sto.s3Client.GetObject(params)
	if err != nil {
		return nil, 0, err
	}

	return resp.Body, uint32(*resp.ContentLength), err
}

func (sto *s3Storage) SubFetch(br blob.Ref, offset, length int64) (rc io.ReadCloser, err error) {
	if offset < 0 || length < 0 {
		return nil, blob.ErrNegativeSubFetch
	}

	rangeValue := fmt.Sprintf("bytes=%d-", offset)
	if length >= 0 {
		rangeValue = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}

	// set GetObjectInput.Range to a range request with offset and length
	params := &s3.GetObjectInput{
		Bucket: aws.String(sto.bucket),
		Key:    aws.String(sto.dirPrefix + br.String()),
		Range:  aws.String(rangeValue),
	}

	resp, err := sto.s3Client.GetObject(params)
	if err == nil {
		return resp.Body, err
	}

	if reqErr, ok := err.(awserr.RequestFailure); ok {
		if reqErr.StatusCode() == 416 {
			return nil, blob.ErrOutOfRangeOffsetSubFetch
		}
	}

	return nil, err
}
