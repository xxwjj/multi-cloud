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

package datastore

import (
	"context"
	"fmt"
	"io"

	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/aliyun"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/aws"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/hws"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

// Init function can perform some initialization work of different datastore.
func Init(backend *backendpb.BackendDetail) DataStoreAdapter {
	var StoreAdapter DataStoreAdapter

	switch backend.Type {
	case "aliyun":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = aliyun.Init(backend)
		return StoreAdapter
	case "obs":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = hws.Init(backend)
		return StoreAdapter
	case "aws":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = aws.Init(backend)
		return StoreAdapter
	default:
		fmt.Printf("Can't find datastore driver %s!\n", backend.Type)
	}
	return nil
}

func Exit(backendType string) {

}

type DataStoreAdapter interface {
	PUT(stream io.Reader, object *pb.Object, context context.Context) S3Error
	DELETE(object *pb.DeleteObjectInput, context context.Context) S3Error
}
