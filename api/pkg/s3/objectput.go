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

package s3

import (
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	//	"github.com/micro/go-micro/errors"
	"strconv"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//ObjectPut -
func (s *APIService) ObjectPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")

	ctx := context.WithValue(request.Request.Context(), "operation", "upload")

	log.Logf("Received request for create bucket: %s", bucketName)

	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	s3err := client.PUT(request.Request.Body, &object, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		res,err := s.s3Client.UpdateObject(ctx,&object)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Log("Upload object successfully.")
		response.WriteEntity(res)
	} else {
		res, err := s.s3Client.CreateObject(ctx, &object)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Log("Upload object successfully.")
		response.WriteEntity(res)
	}

}
