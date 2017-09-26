# Copyright (c) 2015 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
import itertools
import uuid
import daiquiri
from oslo_config import cfg
import six
import operator
import ConfigParser
import time
import json
from gnocchi import incoming
from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils
import optparse
from proton.handlers import MessagingHandler
from proton.reactor import Container


LOG = daiquiri.getLogger(__name__)


class Amqp(MessagingHandler):
    def __init__(self,url, count,conf):
        super(Amqp, self).__init__()
        self.url=url
        self.expected = count
        self.received = 0
        self.conf = conf
        self.incoming = incoming.get_driver(self.conf)
        self.indexer = indexer.get_driver(self.conf)
        self.gauges = {}
        self.counters = {}
        self.absolute = {}
        self.conf = service.prepare_service()
        self.peer_close_is_error = True

    def reset(self):
        self.gauges.clear()
        self.counters.clear()
        self.absolute.clear()


    def on_start(self, event):
        event.container.create_receiver(self.url)



    def on_message(self, event):
        if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            return
        if self.expected == 0 or self.received < self.expected:
            parsed_json=json.loads(event.message.body)
            msgbody=parsed_json[0]
            host_id = "amqp_collectd:" + parsed_json["host"].replace("/", "_")
            #host=parsed_json["host"]
            #measures = {host_id: collections.defaultdict(list)}
            i=0
            metric_type = msgbody["dstypes"][0]
            for value in msgbody["values"]:
                try:
                    self.amqp.treat_metric(host_id,self._serialize_identifier(i,msgbody), metric_type,
                                            value, msgbody["interval"])
                    i+=1
                except Exception as e:
                    LOG.error("Unable to treat metric %s: %s", message, str(e))
            self.received += 1
        if self.received == self.expected:
            event.receiver.close()
            event.connection.close()

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
        try:
            self.indexer.create_resource('amqp_collectd',
                                         host_id,
                                         self.conf.amqpd.creator)
        except indexer.ResourceAlreadyExists:
            LOG.debug("Resource %s already exists",
                      host_id)
        else:
            LOG.info("Created resource %s", host_id)


    def _ensure_resource_type_exists(self):
        try:
            self.indexer.create_resource_type("amqp_collectd")
        except exceptions.ResourceTypeAlreadyExists:
            pass

    def treat_metric(self, resource_id,metric_name, metric_type, value, sampling):

        if metric_type == "absolute":
            if sampling is not None:
                raise ValueError(
                    "Invalid sampling for ms: `%d`, should be none"
                    % sampling)

            if resource_id not in self.absolute:
                self.absolute[resource_id]=collections.defaultdict(list)

            self.absolute[resource_id][metric_name] = storage.Measure(
                utils.dt_in_unix_ns(utils.utcnow()), value)
        elif metric_type == "guage":
            if sampling is not None:
                raise ValueError(
                    "Invalid sampling for g: `%d`, should be none"
                    % sampling)
            if resource_id not in self.gauges:
                self.gauges[resource_id]=collections.defaultdict(list)

            self.gauges[resource_id][metric_name] = storage.Measure(
                utils.dt_in_unix_ns(utils.utcnow()), value)
        elif metric_type == "counter":
            sampling = 1 if sampling is None else sampling
            if resource_id not in self.counters:
                self.counters[resource_id]=collections.defaultdict(list)
            if metric_name in self.counters[resource_id]:
                current_value = self.counters[resource_id][metric_name].value
            else:
                current_value = 0
            self.counters[resource_id][metric_name] = storage.Measure(
                utils.dt_in_unix_ns(utils.utcnow()),
                current_value + (value * (1 / sampling)))
        # TODO(jd) Support "set" type
        # elif metric_type == "s":
        #     pass
        else:
            raise ValueError("Unknown metric type `%s'" % metric_type)

    def flush(self):

        #resources=set(itertools.chain(counter,guage,absolute))

        #for resoource_id in resources:
        for resurce_id, metrics in itertools.chain(
                             six.iteritems(counter),
                             six.iteritems(guage),
                            six.iteritems(absolute)):

             resource = self.indexer.get_resource('amqp_collectd',
                                                  resource_id,
                                                  with_metrics=True)
             for metric_name in metrics:
                 try:
                    metric = resource.get_metric(metric_name)
                    if not metric:
                        ap_name = self._get_archive_policy_name(metric_name)
                        metric = self.indexer.create_metric(
                            uuid.uuid4(),
                            self.conf.amqpd.creator,
                            archive_policy_name=ap_name,
                            name=metric_name,
                            resource_id=resurce_id)
                    self.incoming.add_measures(metric, (metrics[metric_name],))
                 except Exception as e:
                    LOG.error("Unable to add measure %s: %s",
                              metric_name, e)

        self.reset()


    def _get_archive_policy_name(self, metric_name):
        if self.conf.amqpd.archive_policy_name:
            return self.conf.amqpd.archive_policy_name
        # NOTE(sileht): We didn't catch NoArchivePolicyRuleMatch to log it
        ap = self.indexer.get_archive_policy_for_metric(metric_name)
        return ap.name

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-a", "--address", default="localhost:5672/examples",
                  help="address from which messages are received (default %default)")
parser.add_option("-m", "--messages", type="int", default=100,
                  help="number of messages to receive; 0 receives indefinitely (default %default)")

conf = service.prepare_service()
#if conf.statsd.resource_id is None:
    #raise cfg.RequiredOptError("resource_id", cfg.OptGroup("amqpd"))

try:
    Container(Amqp(opts.address,opts.messages,conf)).run()
except KeyboardInterrupt: pass
