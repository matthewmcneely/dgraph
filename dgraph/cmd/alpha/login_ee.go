// +build !oss

/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package alpha

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/dgraph-io/dgraph/edgraph"
	"github.com/dgraph-io/dgraph/x"
	"github.com/golang/glog"
)

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if commonHandler(w, r) {
		return
	}

	// Pass in PoorMan's auth, IP information if present.
	ctx := x.AttachRemoteIP(context.Background(), r)
	ctx = x.AttachAuthToken(ctx, r)

	body := readRequest(w, r)
	loginReq := api.LoginRequest{}
	if err := json.Unmarshal(body, &loginReq); err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	resp, err := (&edgraph.Server{}).Login(ctx, &loginReq)
	if err != nil {
		x.SetStatusWithData(w, x.ErrorInvalidRequest, err.Error())
		return
	}

	jwt := &api.Jwt{}
	if err := jwt.Unmarshal(resp.Json); err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
	}

	response := map[string]interface{}{}
	mp := make(map[string]string)
	mp["accessJWT"] = jwt.AccessJwt
	mp["refreshJWT"] = jwt.RefreshJwt
	response["data"] = mp

	js, err := json.Marshal(response)
	if err != nil {
		x.SetStatusWithData(w, x.Error, err.Error())
		return
	}

	if _, err := x.WriteResponse(w, r, js); err != nil {
		glog.Errorf("Error while writing response: %v", err)
	}
}

func init() {
	http.HandleFunc("/login", loginHandler)
}
