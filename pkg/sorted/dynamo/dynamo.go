/*
Copyright 2013 The Camlistore Authors.

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

// Package dynamo provides an implementation of sorted.KeyValue
// using DynamoDB.
package dynamo

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"camlistore.org/pkg/sorted"
	"go4.org/jsonconfig"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	hashKey  = "h"
	rangeKey = "k"
	valueKey = "v"
	noValue  = ""
)

func init() {
	sorted.RegisterKeyValue("dynamo", newKeyValueFromJSONConfig)
}

type loggerType bool

func (l loggerType) Println(args ...interface{}) {
	if l {
		log.Println(args...)
	}
}

func (l loggerType) Printf(fmt string, args ...interface{}) {
	if l {
		log.Printf(fmt, args...)
	}
}

var (
	logger = loggerType(false)
)

func newKeyValueFromJSONConfig(cfg jsonconfig.Obj) (sorted.KeyValue, error) {
	table := cfg.RequiredString("table")
	accessKey := cfg.RequiredString("aws_access_key")
	secret := cfg.RequiredString("aws_secret_access_key")
	region := cfg.OptionalString("region", "")

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	db, err := db_table(table, accessKey, secret, region)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func db_table(table, accessKey, secret, region string) (*keyValue, error) {
	awsConfig := aws.NewConfig()
	if region != "" {
		awsConfig = awsConfig.WithRegion(region)
	}
	if accessKey != "" {
		awsConfig = awsConfig.WithCredentials(credentials.NewStaticCredentials(accessKey, secret, ""))
	}

	sess := session.New(awsConfig)
	kv := keyValue{Table: table, HashPrefix: "sha1-", HashBegin: 5, HashEnd: 6, db: dynamodb.New(sess)}

	params := &dynamodb.DescribeTableInput{
		TableName: aws.String(kv.Table),
	}

	_, err := kv.db.DescribeTable(params)
	if err != nil {
		return nil, err
	}

	return &kv, nil
}

func (kv *keyValue) hash(curr string, begin bool) string {
	i := strings.Index(curr, kv.HashPrefix)
	if i >= 0 {
		return curr
	}

	last := curr[len(curr)-1]

	if begin {
		if last == ':' || last == '|' {
			return curr + kv.HashPrefix + "0000000000000000000000000000000000000000"
		}
	} else {
		if last == ';' || last == '}' {
			curr = curr[:len(curr)-1] + string([]byte{last - 1})
			return curr + kv.HashPrefix + "ffffffffffffffffffffffffffffffffffffffff"
		}
	}

	log.Println("can't create prefix for", curr)
	return ""
}

func (kv *keyValue) next(curr, last string) string {
	i := strings.Index(curr, kv.HashPrefix)
	if i < 0 {
		log.Println("no next for", curr)
		return ""
	}

	ch := curr[i+kv.HashBegin : i+kv.HashEnd]
	lh := last[i+kv.HashBegin : i+kv.HashEnd]
	rest := curr[i+kv.HashEnd:]

	if ch >= lh {
		return "" // done
	}

	ci, _ := strconv.ParseUint(ch, 16, 64)
	ci += 1

	return fmt.Sprintf("%v%0*x%v", curr[:i+kv.HashBegin], kv.HashEnd-kv.HashBegin, ci, rest)
}

//
// item returns an object with hash and range value set according to the input key.
// this can be used either to get a record or put a record (if there is a value part)
//
func (kv *keyValue) item(key, value string) map[string]*dynamodb.AttributeValue {
	h := key

	i := strings.Index(key, kv.HashPrefix)
	if i >= 0 {
		if kv.HashBegin < 0 || kv.HashEnd <= kv.HashBegin {
			panic("invalid hash location")
		}
		if i+kv.HashEnd > len(key) {
			panic("key too short " + key)
		}

		h = key[:i+kv.HashEnd]
	}

	ret := map[string]*dynamodb.AttributeValue{
		hashKey:  &dynamodb.AttributeValue{S: aws.String(h)},
		rangeKey: &dynamodb.AttributeValue{S: aws.String(key)},
	}

	if value != noValue {
		ret[valueKey] = &dynamodb.AttributeValue{S: aws.String(value)}
	}

	return ret
}

// implementation of KeyValue
type keyValue struct {
	Table      string
	HashPrefix string
	HashBegin  int
	HashEnd    int

	db *dynamodb.DynamoDB
}

type batch interface {
	Mutations() []sorted.Mutation
}

func (kv *keyValue) BeginBatch() sorted.BatchMutation {
	logger.Println("BeginBatch")
	return sorted.NewBatchMutation()
}

func (kv *keyValue) CommitBatch(bm sorted.BatchMutation) error {
	logger.Println("CommitBatch", bm)

	b, ok := bm.(batch)
	if !ok {
		return errors.New("invalid batch type")
	}

	requests := []*dynamodb.WriteRequest{}

	for _, m := range b.Mutations() {
		var wr dynamodb.WriteRequest

		if m.IsDelete() {
			wr.DeleteRequest = &dynamodb.DeleteRequest{Key: kv.item(m.Key(), noValue)}
		} else {
			if err := sorted.CheckSizes(m.Key(), m.Value()); err != nil {
				log.Printf("Skipping storing (%q:%q): %v", m.Key(), m.Value(), err)
				continue
			}

			wr.PutRequest = &dynamodb.PutRequest{Item: kv.item(m.Key(), m.Value())}
		}

		requests = append(requests, &wr)
	}

	logger.Printf("CommitBatch %d items", len(requests))

	for len(requests) > 0 {
		var writebatch []*dynamodb.WriteRequest

		if len(requests) > 25 { // max size of batch list
			writebatch, requests = requests[:25], requests[25:]
		} else {
			writebatch, requests = requests, requests[:0]
		}

		params := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{kv.Table: writebatch},
		}

		res, err := kv.db.BatchWriteItem(params)
		if err != nil {
			logger.Println("BatchWriteItem", err)
			return nil
		}

		if len(res.UnprocessedItems) > 0 {
			log.Printf("reprocessing %d items: %s", len(res.UnprocessedItems), res.UnprocessedItems)
			requests = append(res.UnprocessedItems[kv.Table], requests...)
		}
	}

	return nil
}

func (kv *keyValue) Close() error {
	logger.Println("Close")
	return nil
}

func (kv *keyValue) Set(key, value string) error {
	logger.Println("Set", key, value)

	item := kv.item(key, value)

	params := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(kv.Table),
	}

	_, err := kv.db.PutItem(params)
	return err
}

func (kv *keyValue) Get(key string) (string, error) {
	item := kv.item(key, noValue)

	params := &dynamodb.GetItemInput{
		Key:       item,
		TableName: aws.String(kv.Table),
	}

	res, err := kv.db.GetItem(params)
	logger.Println("Get", key, "error:", err, "item:", res.Item)

	if err != nil {
		return "", err
	} else if len(res.Item) > 0 {
		return aws.StringValue(res.Item[valueKey].S), nil
	} else {
		return "", sorted.ErrNotFound
	}
}

// Delete removes the document with the matching key.
func (kv *keyValue) Delete(key string) error {
	logger.Println("Delete", key)

	item := kv.item(key, noValue)
	params := &dynamodb.DeleteItemInput{
		Key:       item,
		TableName: aws.String(kv.Table),
	}

	_, err := kv.db.DeleteItem(params)
	return err
}

// Wipe removes all documents from the collection.
func (kv *keyValue) Wipe() error {
	logger.Println("Wipe")

	logger.Println("DescribeTable...")

	descParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(kv.Table),
	}
	resp, err := kv.db.DescribeTable(descParams)
	if err != nil {
		return err
	}

	logger.Println("DeleteTable...")

	delParams := &dynamodb.DeleteTableInput{
		TableName: aws.String(kv.Table),
	}
	if _, err := kv.db.DeleteTable(delParams); err != nil {
		return err
	}

	for {
		sresp, err := kv.db.DescribeTable(descParams)
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				break // deleted
			}
		}
		if err != nil {
			return err
		}

		logger.Println(aws.StringValue(sresp.Table.TableStatus))
		time.Sleep(2 * time.Second)
	}

	logger.Println("CreateTable...")

	createParams := &dynamodb.CreateTableInput{
		TableName: aws.String(kv.Table),

		AttributeDefinitions: resp.Table.AttributeDefinitions,
		KeySchema:            resp.Table.KeySchema,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  resp.Table.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: resp.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}

	if _, err := kv.db.CreateTable(createParams); err != nil {
		return err
	}

	for {
		sresp, err := kv.db.DescribeTable(descParams)
		if err != nil {
			return err
		}

		status := aws.StringValue(sresp.Table.TableStatus)

		if status == dynamodb.TableStatusActive {
			break
		}

		logger.Println(status)
		time.Sleep(2 * time.Second)
	}

	logger.Println("Done")
	return nil
}

func (kv *keyValue) Find(start, end string) sorted.Iterator {
	logger.Println("Find", start, end)

	it := &iter{
		kv:     kv,
		curr:   kv.hash(start, true),
		last:   kv.item(kv.hash(end, false), noValue),
		rindex: -1,
	}

	it.req.TableName = aws.String(kv.Table)
	it.req.KeyConditionExpression = aws.String("h = :hash AND k >= :start")
	it.req.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{}
	return it
}

// Implementation of Iterator
type iter struct {
	kv *keyValue

	curr string                              // current key to search
	last map[string]*dynamodb.AttributeValue // hash and key for last key

	req    dynamodb.QueryInput
	res    *dynamodb.QueryOutput
	rindex int
}

func (it *iter) Next() bool {
	logger.Println("Next", it)

	if it.res != nil {
		it.rindex += 1

		if it.rindex < len(it.res.Items) { // results still available
			return true
		}
	}

	if it.curr == "" { // done
		return false
	}

	it.rindex = -1 // reset position

	last := false

	for {
		item := it.kv.item(it.curr, noValue)

		it.req.ExpressionAttributeValues[":hash"] = item[hashKey]
		it.req.ExpressionAttributeValues[":start"] = item[rangeKey]

		if it.res != nil && len(it.res.LastEvaluatedKey) != 0 {
			it.req.ExclusiveStartKey = it.res.LastEvaluatedKey
		} else {
			it.req.ExclusiveStartKey = nil
			it.curr = it.kv.next(it.curr, aws.StringValue(it.last[rangeKey].S))

			if it.curr == "" {
				it.req.KeyConditionExpression = aws.String("h = :hash AND k BETWEEN :start AND :end")
				it.req.ExpressionAttributeValues[":end"] = it.last[rangeKey]
				last = true
			}
		}

		var err error

		log.Println("Query", it.req)

		it.res, err = it.kv.db.Query(&it.req)
		if err != nil {
			log.Println("error", err)
			it.curr = ""
			it.res = nil
			return false
		}

		//log.Println("response", it.res)

		if aws.Int64Value(it.res.Count) > 0 {
			it.rindex = 0
			return true
		}

		if last {
			break
		}
	}

	return false
}

func (it *iter) Key() string {
	item := it.res.Items[it.rindex]
	return aws.StringValue(item[rangeKey].S)
}

func (it *iter) KeyBytes() []byte {
	return []byte(it.Key())
}

func (it *iter) Value() string {
	item := it.res.Items[it.rindex]
	return aws.StringValue(item[valueKey].S)
}

func (it *iter) ValueBytes() []byte {
	return []byte(it.Value())
}

func (it *iter) Close() error {
	it.kv = nil
	it.curr = ""
	it.res = nil
	it.rindex = -1
	return nil
}
