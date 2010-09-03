#!/usr/bin/env python

__author__ = "David Stainton"
__copyright__ = "Copyright 2010 Digg, Inc."

import os
import sys
from libcmd import read_cmd
from optparse import OptionParser
import re

def main():

    fincore_path = '~/fincore'
    usage = '%prog [options] <cassandra-data-directory>'
    parser = OptionParser(usage=usage)
    parser.add_option('-c', '--columnfamily-summarize', dest='columnfamily', action='store_true', default=False,
                      help="Summarize cached Cassandra data on a per Column Family basis.")
    parser.add_option('--exclude-filter', dest='exclude_filter', action='store_true', default=False,
                      help="Exclude statistics for Cassandra Filter files.")
    parser.add_option('--exclude-index', dest='exclude_index', action='store_true', default=False,
                      help="Exclude statistics for Cassandra Index files.")
    parser.add_option('--exclude-data', dest='exclude_data', action='store_true', default=False,
                      help="Exclude statistics for Cassandra Data files.")
    options, args = parser.parse_args()

    if len(args) < 1:
        parser.print_help()
        return 1

    if options.exclude_filter and options.exclude_index and options.exclude_data:
        print "Cannot exclude all three: data, filter and index."
        print "Exclude just one or two of them!"
        return 1

    filetypes = {"Index":0,"Filter":0,"Data":0}

    if options.exclude_filter:
        del filetypes["Filter"]
    if options.exclude_index:
        del filetypes["Index"]
    if options.exclude_data:
        del filetypes["Data"]

    filetypes_filter = "|".join(filetypes.keys())

    data_dir = args[0]
    fileList = []
    for (path, dirs, files) in os.walk(data_dir):
        for file in files:
            fileList.append(os.path.join(path,file))
        break

    files = ' '.join(fileList)
    buf = read_cmd('%s --pages=false --only-cached %s' % (fincore_path, files))
    lines = buf.split('\n')
    lines.pop(0) # get rid of output column header

    if options.columnfamily:
        filetypes = {"Index":0,"Filter":0,"Data":0}

        # data files look like this: Counts-2374-Data.db 
        cassandra_file = re.compile("^.+/(\w+)-\d+-(%s)\.db$" % filetypes_filter)
        columnfamily_hash = {}
        for line in lines:
            # skip lines with errors
            if ':' in line:
                continue
            fields = line.split()
            if len(fields) == 6:
                (filename,size,total_pages,cached_pages,cached_size,cached_percent) = fields
            else:
                break
            mymatch = cassandra_file.match(filename)
            if mymatch:
                if columnfamily_hash.has_key(mymatch.group(1)):
                    columnfamily_hash[mymatch.group(1)] += int(cached_size)
                else:
                    columnfamily_hash[mymatch.group(1)] = int(cached_size)
            else:
                pass
                #print "fix bug: %s: no match" % filename
                #return 0

        for k,v in sorted(columnfamily_hash.items(), key=lambda x:x[1], reverse=True):
            print "%s %s" % (k,v)


    return 0

if __name__ == '__main__':
    sys.exit(main())

