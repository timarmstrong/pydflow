# Copyright 2010-2011 Tim Armstrong <tga@uchicago.edu>
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import namedtuple
import struct


REQ_SIZE = 12


# Maximum size of request in bytes to avoid enormous buffers
MAX_REQ_LEN = 2**20





# tag => identifier for job
req_header = namedtuple('creq', ['tag', 'flg', 'len'])

# flag => bitwise flags for type of message
REPLY_FLAG = 0x00000001
FINAL_FLAG = 0x00000002
ERROR_FLAG = 0x00000004
PROGRESSIVE_FLAG = 0x00000008

# commands the coaster worker understands
worker_commands = set('SHUTDOWN', 'SUBMITJOB', 'REGISTER', 'HEARTBEAT',
                'WORKERSHELLCMD')

#
worker_replies = set('PUT', 'JOBSTATUS', 'OK', 'HEARTBEAT', 'GET')


def parseHeader(arr):
    """
    Array of 4 byte unsigned ints
    """
    x = struct.unpack('<LLL')
    # TODO: check bf len is 12 bytes
    return req_header(*x)

def mkHeader(tag, flg, length):
    return struct.pack('<LLL', tag, flg, length)


def sendCoasterMsg(sock, tag, flag, data):
    sock.send(mkHeader(tag, flag, len(data)))
    sock.send(data)















rbuf = array('B', REQ_SIZE)
def handleReq(sock):
    # Receive header
    sock.recv_into(rbuf, REQ_SIZE)
    hdr = parseHeader(rbuf)
    
    data = reliable_recv(sock, hdr.len)

def reliable_recv(sock, mlen)
    """
    Reads mlen bytes from socket, repeatedly calling recv if needed.
    """
    if mlen > MAX_REQ_LEN:
        raise Exception("%d too many bytes to read all at once", mlen)
    buf = array('B', mlen)
    got = 0
    frags = []
    while got < mlen:
        amt = min(mlen - got, 4096)
        frag = sock.recv(amt)
        frags.append(frag)
        got += len(frag)
    return ''.join(frags)

1G
