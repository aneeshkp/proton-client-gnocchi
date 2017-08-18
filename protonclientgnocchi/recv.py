#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from __future__ import print_function
import optparse
from proton.handlers import MessagingHandler
from proton.reactor import Container
import gnocchi
class Recv(MessagingHandler):
    def __init__(self, url, count,enable_gnocchi):
        super(Recv, self).__init__()
        self.url = url
        self.expected = count
        self.received = 0
        self.enable_gnocchi  = enable_gnocchi
        if enable_gnocchi==1:
	   self.gnocchi=gnocchi.Gnocchi()
           self.gnocchi.config()
           self.gnocchi.init()

    def on_start(self, event):
        event.container.create_receiver(self.url)

    def on_message(self, event):
        if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            return
        if self.expected == 0 or self.received < self.expected:
            if self.enable_gnocchi==1:
	       self.gnocchi.write(event.message.body)
            else :
	       print(event.message.body)
            self.received += 1
            if self.received == self.expected:
                event.receiver.close()
                event.connection.close()

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-a", "--address", default="localhost:5672/examples",
                  help="address from which messages are received (default %default)")
parser.add_option("-m", "--messages", type="int", default=100,
                  help="number of messages to receive; 0 receives indefinitely (default %default)")
parser.add_option("-g", "--gnocchi", type="int", default=0,
                  help="1 is for write to gnocchi; 0 print to console (default %default)")

opts, args = parser.parse_args()

try:
    Container(Recv(opts.address, opts.messages, opts.gnocchi)).run()
except KeyboardInterrupt: pass




