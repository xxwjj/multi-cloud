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

package job

import (
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/Filters/context"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
)

func Create(ctx *context.Context, job *Job) error {
	jobId := bson.NewObjectId()
	job.Id = jobId

	err := db.DbAdapter.CreateJob(ctx, job)
	for i := 0; i < 3; i++ {
		if err == nil || err == ERR_DB_ERR {
			return err
		}
		//Otherwise err is ERR_DB_IDX_DUP
		jobId = bson.NewObjectId()
		job.Id = jobId
		err = db.DbAdapter.CreateJob(ctx, job)
	}

	log.Log("Add job failed, objectid duplicate too much times.")
	return ERR_INNER_ERR
}

func Get(ctx *context.Context, id string) (*Job, error) {
	return db.DbAdapter.GetJob(ctx, id)
}

func List(ctx *context.Context) ([]Job, error) {
	return db.DbAdapter.ListJob(ctx)
}
