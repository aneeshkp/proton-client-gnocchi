# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import collections
import itertools
import traceback
import sys
from gnocchiclient import auth
from config import Config
from gnocchiclient.v1 import client
from gnocchiclient import exceptions
from keystoneauth1 import session
import operator
import ConfigParser
import time   
import json
class Gnocchi(object):
    def config(self):
        # NOTE(sileht): Python threading system is not yet initialized here
        # FIXME(sileht): We handle only one configuration block for now
        self.conf  = ConfigParser.ConfigParser()
        self.conf.read("amqp-gnocchi.conf")

       # ict((c.key.lower(), c.values[0]) for c in
       #                  config.children)

    def init(self):
        auth_plugin = auth.GnocchiBasicPlugin(user="admin",endpoint="http://localhost:8041")
        self.g= client.Client(session_options={'auth': auth_plugin})
       # s = session.Session(auth=auth)
       # self.g = client.Client(
       #     1, s,
       #     interface=self.conf.get('api','interface'),
       #     region_name=self.conf.get('api','region_name'),
       #     endpoint_override=self.conf.get('api','endpoint'))
       # print(self.conf.get('api','endpoint'));
        self._resource_type = "qpid_amqp"
        try:
            self.g.resource_type.get(self._resource_type)
        except exceptions.ResourceTypeNotFound:
            self.g.resource_type.create({
                "name": self._resource_type,
                "attributes": {
                    "host": {
                        "required": True,
                        "type": "string",
                    },
                },
            })

        self.values = []
        self.batch_size = self.conf.get('api',"batchsize")

    @staticmethod
    def _serialize_identifier(index, v):
        """Based of FORMAT_VL from collectd/src/daemon/common.h.
        The biggest difference is that we don't prepend the host and append the
        index of the value, and don't use slash.
        """
        return (v["plugin"] + ("-" + v["plugin_instance"]
                            if v["plugin_instance"] else "")
                + "@"
                + v["type"] + ("-" + v["type_instance"]
                            if v["type_instance"] else "")
                + "-" + str(index))        

    def _ensure_resource_exists(self, host_id, host):
        attrs = {"id": host_id, "host": host}
        try:
            try:
                self.g.resource.create(self._resource_type, attrs)
            except exceptions.ResourceTypeNotFound:
                self._ensure_resource_type_exists()
                self.g.resource.create(self._resource_type, attrs)
        except exceptions.ResourceAlreadyExists:
            pass

    def _ensure_resource_type_exists(self):
        try:
            self.g.resource_type.create({
                "name": self._resource_type,
                "attributes": {
                    "host": {
                        "required": True,
                        "type": "string",
                    },
                },
            })
        except exceptions.ResourceTypeAlreadyExists:
            pass

    def write(self, metric_values):
        #if len(self.values) >= self.batch_size:
        parsed_json=json.loads(metric_values)
        print(json.dumps(metric_values))
        print("\n");
        print("----------------------------------------\n")
        host_id = "qpid_amqp:" + parsed_json[0]["host"].replace("/", "_")  
        host=parsed_json[0]["host"]
        measures = {host_id: collections.defaultdict(list)} 
        print(host_id);
        print("values\n")
        i=0
        for value in parsed_json[0]["values"]:
            measures[host_id][self._serialize_identifier(i,parsed_json[0])].append({"timestamp":parsed_json[0]["time"],"value":value,})
            i+=1
        try:
            print(measures)
            self.g.metric.batch_resources_metrics_measures(
            measures, create_metrics=True)
        except exceptions.BadRequest:
            # Create the resource and try again
            self._ensure_resource_exists(host_id, host)
            self.g.metric.batch_resources_metrics_measures(
            measures, create_metrics=True)
            
