#!/usr/bin/python
# encoding: utf-8
'''
squeue(rc)  -- An RC squeue query tool that interrogates a central Slurm squeue output

@author:     Aaron Kitzmiller

@copyright:  2015 Harvard University. All rights reserved.

@license:    GPL v 2.0

@contact:    aaron_kitzmiller@harvard.edu
@deffield    updated: Updated
'''

import sys, subprocess, traceback, socket
import os, time, re
import MySQLdb

from argparse import ArgumentParser
from argparse import ArgumentDefaultsHelpFormatter 

__all__ = []
__version__ = 0.1
__date__ = '2015-03-27'
__updated__ = '2015-05-28'

DEBUG = os.environ.get("SQUEUE_DEBUG", False)

LOOP_INTERVAL = 10
CONNECTION_WAIT = 2
MAX_ATTEMPTS = 5
# QUEUE_FORMAT = "%14i %12P %12j %16u %20V %10S %2t %10M %.6C %.6D %.8m %20R %n "
JOB_STATE_CODES = {
    "PENDING"       : "PD",
    "RUNNING"       : "R", 
    "SUSPENDED"     : "S",
    "CANCELLED"     : "CA",
    "COMPLETING"    : "CG",
    "COMPLETED"     : "CD",
    "CONFIGURING"   : "CF",
    "FAILED"        : "F",
    "TIMEOUT"       : "TO",
    "PREEMPTED"     : "PR",
    "NODE_FAIL"     : "NF",
    "SPECIAL_EXIT"  : "SE",
    "REQUEUE_HOLD"  : "RH",
}
SQUEUE_FIELDS = {
    "a" : "account",
    "b" : "gres",
    "c" : "min_cpus",
    "d" : "min_tmp_disk",
    "e" : "end_time",
    "f" : "features",
    "g" : "group",
    "h" : "shared",
    "i" : "jobid",
    "j" : "name",
    "k" : "comment",
    "l" : "timelimit",
    "m" : "min_memory",
    "n" : "req_nodes",
    "o" : "command",
    "Q" : "priority",
    "q" : "qos",
    "r" : "reason",
    "t" : "state", 
    "u" : "user",
    "v" : "reservation",
    "y" : "nice",
    "B" : "exec_host",
    "C" : "cpus",
    "D" : "nodes",
    "E" : "dependency",
    "F" : "array_job_id",
    "K" : "array_task_id",
    "L" : "time_left",
    "M" : "time",
    "N" : "node_list",
    "O" : "contiguous",
    "P" : "partition",
    "R" : "nodelistreason",
    "S" : "start_time",
    "T" : "state",
    "U" : "user",
    "V" : "submit_time",
    "W" : "licenses", 
    "Z" : "work_dir", 
}

LONG_FORMAT = "%.18i %.9P %.8j %.8u %.8T %.10M %.9l %.6D %R"


program_name = os.path.basename(sys.argv[0])
program_version = "v%s" % __version__
program_build_date = str(__updated__)
program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
supported_options = "Only -u/--user, -o/--format, -l/--long, --noheader, -p/--partition, -j/--jobs, -t/--states, -h/--noheader and -w/--nodelist switches are currently supported.  A call of squeue with other options (e.g. -r) will fall back to /usr/bin/squeue"
using_slurm_squeue = "To use the Slurm squeue, specify the full path /usr/bin/squeue"
program_shortdesc = """squeue(rc) An squeue replacement that interrogates a shared squeue result, 
reducing load on the Slurm controller. Results for the current user are shown by default.  

%s

%s
""" % (supported_options, using_slurm_squeue)
program_license = """%s

  Created by Aaron Kitzmiller on %s.
  Copyright 2015 Harvard University. All rights reserved.

  Licensed under the GNU Public License v 2.0
  https://www.gnu.org/licenses/gpl-2.0.html

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
""" % (program_shortdesc, str(__date__))


def decodeNodeList(nodeliststring):
    """
    Converts Slurm nodelist encoding into a newline-separated list
    """
    strs = []
    pat = re.compile(r'([0-9a-zA-Z]+)\[([^\]]+)\]')
    result = pat.search(nodeliststring)
    if result is not None:
        # If there is a ...[n,n-n,n] then process that
        it = pat.finditer(nodeliststring)
        for match in it:
            prefix = match.group(1)
            suffixes = match.group(2).split(",")
            for suffix in suffixes:
                if "-" in suffix:
                    [min, max] = suffix.split("-")
                    length = len(min)
                    formatstr = "{0:0%dd}" % length
                    j = int(min)
                    while formatstr.format(j) != max:
                        strs.append("%s%s" % (prefix, formatstr.format(j)))
                        j += 1
                    strs.append("%s%s" % (prefix, max))
                else:
                    strs.append("%s%s" % (prefix, suffix))                        
    else:
        strs.append(nodeliststring)
        
    str = "\n".join(strs)
    if DEBUG:
        sys.stderr.write("Decoding of %s\n%s\n" % (nodeliststring, str))
    return str
    
    
def getHeaderForColumn(column):
    """
    Converts column to output header
    """
    if column == "nodelistreason":
        return "NODELIST(REASON)"
    else:
        return column.upper()
    

def fail2squeue():
    """
    Use /usr/bin/squeue with sys.argv.  Used when unrecognized arguments are passed in. 
    """ 
    cmd = "/usr/bin/squeue %s " % ' '.join(sys.argv[1:])
    return subprocess.call(cmd, shell=True)


class ErrorMessageArgumentParser(ArgumentParser):
    def error(self, message):
        if "unrecognized arguments" in message:
            sys.exit(fail2squeue())
        else:
            super(ErrorMessageArgumentParser, self).error(message)
            
    
def main(argv=None):  # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    # Setup argument parser
    parser = ErrorMessageArgumentParser(description=program_shortdesc, formatter_class=ArgumentDefaultsHelpFormatter, add_help=False)
    parser.add_argument("-u", "--user", help="Get data for the specified user. Comma-separated list is OK. 'all' will retrieve data for all users [default: %s]" % os.environ["USER"])
    parser.add_argument("-w", "--nodelist", dest="nodelist", help="Filter results for the specified nodes.  localhost is mapped to the current host.")
    parser.add_argument("-p", "--partition", dest="partition", help="Filter results for the specified partition.  Comma-separated list is OK.")
    parser.add_argument("-j", "--jobs", dest="jobs", help="Filter results for the specified job.  Comma-separated list is OK.")
    parser.add_argument("-t", "--states", dest="states", help="Filter results a state.  Comma-separated list is OK.  'all' is the default", default="all")
    parser.add_argument("--exact-partition-names", action="store_true", help="Forces exact match of partition names so that, e.g. requeue and serial_requeue are different.", default=False)
    parser.add_argument("-h", "--noheader", dest="noheader", action="store_true", help="Do not print the header.")
    parser.add_argument("--help", action="help", help="This help message.")
    formatoptions = parser.add_mutually_exclusive_group()
    formatoptions.add_argument("-o", "--format", dest="format", help="Format of squeue output. See man squeue for details. %%all is not supported.", default="%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R")
    formatoptions.add_argument("-l", "--long", action="store_true", dest="long", help="Long format squeue output.")
 
    args = parser.parse_args()
    
    # Process user arg
    # If nothing for user was specified on the command line, then
    # things like jobid and partition should not be limited to the current user,
    # so we set this flag for them to use
    using_user_default = False
    user = args.user
    if user is None:
        user = os.environ["USER"]
        using_user_default = True
    if user == "all":
        user = ".*"
        
    # Process states arg
    statesre = ".*"
    if args.states != 'all':
        # Swap the JOB_STATE_CODES dict to that abbreviated states can be searched as full names
        abbrevtrans = dict((v, k) for k, v in JOB_STATE_CODES.iteritems())
        states = args.states.split(',')
        for i, state in enumerate(states):
            if state in abbrevtrans:
                states[i] = abbrevtrans[state]
        statesre = "|".join(states)
     
    # Process node list arg
    nodelistre = None
    if args.nodelist:
        if args.nodelist == 'localhost':
            args.nodelist = socket.gethostname().split(".")[0]
            if DEBUG:
                sys.stderr.write("Hostname is %s\n" % args.nodelist)
        nodesre = ["^%s$" % n for n in args.nodelist.split(',')]
        nodelistre = re.compile("|".join(nodesre), re.MULTILINE)
    partitionre = '.*'
    jobsre = '.*'
    
    # Process partitions
    if args.partition:
        if args.exact_partition_names:
            partitionslist = ["^%s$" % n for n in args.partition.split(',')]
        else:
            partitionslist = args.partition.split(',')
        partitionre = "|".join(partitionslist)
        if using_user_default:
            user = '.*'
        if DEBUG:
            sys.stderr.write("Partition re is %s\n" % partitionre)
        
    # Process jobid arg
    if args.jobs:
        jobslist = ["^%s$" % n for n in args.jobs.split(',')]
        jobsre = "|".join(jobslist)
        if using_user_default:
            user = '.*'
        if DEBUG:
            sys.stderr.write("Jobs re is %s\n" % jobsre)
    
    SQL_DSN = {
        "host"      : os.environ.get("SQUEUE_HOST", "db-internal"),
        "db"        : os.environ.get("SQUEUE_DB", "portal"),
        "user"      : os.environ.get("SQUEUE_USER", "squeuedb"),
        "passwd"    : os.environ.get("SQUEUE_PASSWD", "squeuedb"),
    }
    if DEBUG:
        sys.stderr.write("%s\n" % repr(SQL_DSN))

    sql = """
        select * 
        from  jobs_squeueresults 
        where user regexp %s and
              partition regexp %s and 
              jobid regexp %s and 
              state regexp %s 
    """.translate(None, "\n")
    
    """
    Connection attempt is made.  After MAX_ATTEMPTS tries,
    if a connection cannot be made, then Slurm sqeueue command is run.  
    If an exception occurs during processing, it falls back to Slurm squeue
    """
    connection = None
    connection_attempts = 0
    while connection is None and connection_attempts < MAX_ATTEMPTS:
        try:
            connection = MySQLdb.connect(**SQL_DSN)
        except Exception, e:
            if DEBUG:
                sys.stderr.write("Connection error %s\n%s\n" % (str(e), traceback.format_exc()))
            time.sleep(CONNECTION_WAIT)
            connection_attempts += 1
            
    """
    Make the header and line python format strings with named parameters
    """
    if args.long:
        formatarg = LONG_FORMAT
    else:
        formatarg = args.format
    
    # Check for small t so we know to translate the state results into codes
    has_t = False
    if formatarg.find('t') != -1:
        has_t = True
        
    # Convert the squeue format string into a python .format string
    # Convert %.. into {..}
    formatstr = re.sub(r'%(\.?\d*[a-zA-Z])', r'{\1}', formatarg)
    
    # Convert dots into right justification (>)
    formatstr = re.sub(r'{\.(\d+)', r'{>\1', formatstr)
    
    # Convert one letter codes into column names so that query result dict can be 
    # used in the format statement
    for k, v in SQUEUE_FIELDS.iteritems():
        formatstr = re.sub(r'{([>\d]+)' + k + '}', r'{' + v + r':\1}', formatstr)
        formatstr = formatstr.replace("{%s}" % k, "{%s}" % v)        
        
    # Covert digits into digit.digit
    formatstr = re.sub(r'(\d+)', r'\1.\1', formatstr)
    
    user = '$|^'.join(re.split(r'\s*,\s*', user))
    user = '^%s$' % user

    try:
        if connection is not None:
            cursor = connection.cursor()
            if DEBUG:
                print "sql: %s\nuser: %s\npartition: %s" % (sql, user, partitionre)
            cursor.execute(sql, [user, partitionre, jobsre, statesre])
            desc = cursor.description
            data = [dict(zip([col[0] for col in desc], row)) for row in cursor.fetchall()]
            headers = dict((col[0], getHeaderForColumn(col[0])) for col in desc)
            
            # Print out the header
            if not args.noheader:
                print formatstr.format(**headers)
                
            # Print out each row
            for row in data:
                if "state" in row and has_t:
                    row["state"] = JOB_STATE_CODES[row["state"]]
                # Make sure they're all strings
                for k in row.keys():
                    row[k] = str(row[k])
                    
                passesfilter = True
                
                # Filter by node if needed
                if nodelistre is not None and nodelistre.search(decodeNodeList(row["node_list"])) is None:
                    passesfilter = False
                    
                if passesfilter:    
                    print formatstr.format(**row)
            cursor.close()
            connection.commit()
        else:
            # If there is a problem, fall back to squeue
            cmd = "/usr/bin/squeue --format='%s' -u %s" % (formatarg, user)
            if subprocess.call(cmd, shell=True) != 0:
                return 1
            
    except KeyboardInterrupt:
        return 0
    except Exception, e:
        # If there is a problem, set connection to None and fall back to squeue
        if DEBUG:
            sys.stderr.write("Connection error %s\n%s\n" % (str(e), traceback.format_exc()))
        connection = None
            
    return 0


if __name__ == "__main__":
    sys.exit(main())

