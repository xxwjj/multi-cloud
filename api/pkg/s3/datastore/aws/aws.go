// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

type AwsAdapter struct {
	backend            *backendpb.BackendDetail
	session            *session.Session
	svc                *awss3.S3                          //for multipart upload
	multiUploadInitOut *awss3.CreateMultipartUploadOutput //for multipart upload
	//uploadId string //for multipart upload
	completeParts []*awss3.CompletedPart //for multipart upload
}

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.ak, SecretAccessKey: myc.sk}
	return cred, nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func Init(backend *backendpb.BackendDetail) *AwsAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	region := backend.Region

	s3aksk := s3Cred{ak: AccessKeyID, sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	disableSSL := true
	sess, err := session.NewSession(&aws.Config{
		Region:      &region,
		Endpoint:    &endpoint,
		Credentials: creds,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		return nil
	}

	adap := &AwsAdapter{backend: backend, session: sess}
	return adap
}

func (ad *AwsAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.BucketName + "/" + object.ObjectKey

	if ctx.Value("operation") == "upload" {
		uploader := s3manager.NewUploader(ad.session)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
			Body:   stream,
		})

		if err != nil {
			log.Logf("Upload to aws failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aws failed"}
		}

	}

	return NoError
}

func (ad *AwsAdapter) GET(object *pb.Object, context context.Context) (io.ReadCloser, S3Error) {

	bucket := ad.backend.BucketName
	var buf []byte
	writer := aws.NewWriteAtBuffer(buf)
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	if context.Value("operation") == "download" {
		downloader := s3manager.NewDownloader(ad.session)
		numBytes, err := downloader.DownloadWithContext(context, writer, &awss3.GetObjectInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
		})
		if err != nil {
			log.Logf("Download failed:%v", err)
			return nil, S3Error{Code: 500, Description: "Download failed"}
		} else {
			log.Logf("Download succeed, bytes:%d\n", numBytes)
			log.Logf("Download succeed, writer:%v\n", writer)
			body := bytes.NewReader(writer.Bytes())
			log.Logf("Download succeed, body:%v\n", *body)
			// var ioreader io.Reader
			// ioreader.Read(writer.Bytes())
			// var ioReaderClose io.ReadCloser
			// reader := ioutil.NopCloser(body)
			// ioReaderClose.Read(writer.Bytes())
			ioReaderClose := ioutil.NopCloser(body)
			log.Logf("Download succeed, ioReaderClose:%v\n", ioReaderClose)
			return ioReaderClose, NoError
		}

	}

	return nil, NoError
}

func (ad *AwsAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.Bucket + "/" + object.Key

	svc := awss3.New(ad.session)
	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &newObjectKey}

	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("Delete object %s from aws successfully.\n", newObjectKey)

	return NoError
}

func (ad *AwsAdapter) INITMULTIPARTUPLOAD(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.backend.BucketName
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	ad.svc = awss3.New(ad.session)
	multipartUpload := &pb.MultipartUpload{}
	multiUpInput := &awss3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &newObjectKey,
	}
	res, err := ad.svc.CreateMultipartUpload(multiUpInput)
	if err != nil {
		log.Fatalf("Init s3 multipart upload failed, err:%v\n", err)
		return nil, InternalError
	} else {
		log.Logf("Init s3 multipart upload succeed, UploadId:%s\n", *res.UploadId)
		multipartUpload.Bucket = bucket
		multipartUpload.Key = newObjectKey
		multipartUpload.UploadId = *res.UploadId
		return multipartUpload, NoError
	}
}

func (ad *AwsAdapter) UPLOADPART(stream io.Reader, multipartUpload *pb.MultipartUpload, partNumber int64, upBytes int64, context context.Context) (*pb.Object, S3Error) {
	tries := 1
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Key
	bytess, _ := ioutil.ReadAll(stream)
	upPartInput := &awss3.UploadPartInput{
		Body:          bytes.NewReader(bytess),
		Bucket:        &bucket,
		Key:           &newObjectKey,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      &multipartUpload.UploadId,
		ContentLength: aws.Int64(upBytes),
	}

	//listPartInput := &awss3.ListMultipartUploadsInput{
	//	//Bucket:         &bucket,
	//	//KeyMarker:      &newObjectKey,
	//	//UploadIdMarker: &multipartUpload.UploadId,
	//}
	//resp, err := ad.svc.ListMultipartUploads(nil)
	//log.Logf("list resp:%v, err:%v", resp, err)
	for tries <= 3 {
		ad.svc = awss3.New(ad.session)
		upRes, err := ad.svc.UploadPart(upPartInput)
		if err != nil {
			if tries == 3 {
				log.Logf("[ERROR]Upload part to aws failed. err:%v\n", err)
				return nil, S3Error{Code: 500, Description: "Upload failed"}
			}
			log.Logf("Retrying to upload part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Logf("Uploaded part #%d\n", partNumber)
			part := awss3.CompletedPart{
				ETag:       upRes.ETag,
				PartNumber: aws.Int64(partNumber),
			}
			ad.completeParts = append(ad.completeParts, &part)
			break
		}
	}
	return nil, NoError
}

func (ad *AwsAdapter) COMPLETEMULTIPARTUPLOAD(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Key
	completeInput := &awss3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &newObjectKey,
		UploadId: &multipartUpload.UploadId,
		MultipartUpload: &awss3.CompletedMultipartUpload{
			Parts: ad.completeParts,
		},
	}

	rsp, err := ad.svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
	} else {
		log.Logf("completeMultipartUploadS3 successfully, rsp:%v\n", rsp)
	}
	return NoError
}

func (ad *AwsAdapter) ABORTMULTIPARTUPLOAD(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Key
	abortInput := &awss3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &newObjectKey,
		UploadId: &multipartUpload.UploadId,
	}

	rsp, err := ad.svc.AbortMultipartUpload(abortInput)
	if err != nil {
		log.Logf("abortMultipartUploadS3 failed, err:%v\n", err)
	} else {
		log.Logf("abortMultipartUploadS3 successfully, rsp:%v\n", rsp)
	}
	return NoError
}
