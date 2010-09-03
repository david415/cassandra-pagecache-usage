# Copyright 2009 Tailrank, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License.  You may obtain a copy
# of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations under
# the License.
#
# author: Kevin Burton
#

__author__ = "Kevin Burton"
__copyright__ = "Copyright 2009 Tailrank, Inc."
__license__ = "Apache License"

import sys
from subprocess import *

def read_cmd(cmd,input=None,cwd=None):
    """Run the given command and read its output"""

    pipe = Popen(cmd,shell=True,stdout=PIPE,stderr=PIPE,stdin=PIPE,cwd=cwd)

    out=''
    err=''

    while True: #superfluous while

        (_out,_err) = pipe.communicate( input )

        out += _out
        err += _err

        returncode = pipe.poll()
        
        if returncode == 0:
            return out
        elif returncode >= 0:
            if sys.stderr != None:
                sys.stderr.write(err)
            raise Exception( "%s exited with %s" % (cmd, returncode) )

def watch_cmd(cmd):
    """Run the given OS command and assert that it exits correctly while watching for new data in realtime."""

    #pipe = Popen(cmd,shell=True,stdout=sys.stdout,stderr=PIPE)
    pipe = Popen(cmd,shell=True)

    while True:

        returncode = pipe.poll()

        if returncode == 0:
            return 0
        elif returncode >= 0:
            #err=pipe.stderr.read()
            #sys.stderr.write( err )
            raise Exception( "%s exited with %s" % (cmd, returncode) )

