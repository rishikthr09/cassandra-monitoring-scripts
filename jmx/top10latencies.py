import subprocess
import sys
from operator import itemgetter

# USAGE
# python top10latencies.py <PATH_TO_CQLSH> <KEYSPACE_NAME> <CASSANDRA_PID> <PATH_TO_JMXTERM_JAR>


#constants
temp_cf_file = '/tmp/cfs'
temp_jmx_query_file = '/tmp/store_jmx_query'
metric_type = 'ReadLatency'
metric_name = '99thPercentile'
cassandra_host = '127.0.0.1'
cassandra_port = '9078'
top_number = 10
pad_length = 15


#input
CQLSH_PATH = sys.argv[1]
KEYSPACE_NAME = sys.argv[2]
CASSANDRA_PID = sys.argv[3]
JMX_TERM_JAR = sys.argv[4]


f = open(temp_cf_file,'w')
subprocess.call([CQLSH_PATH, cassandra_host, cassandra_port, '-e', "SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='"+ KEYSPACE_NAME +"';"], stdout=f)
f.close()

all_cf_names = []

f = open(temp_cf_file,'r')
for line in f.readlines():
    line = line.strip()
    if(len(line) == 0):
        continue
    if("columnfamily_name" in line or "-----------" in line or " rows" in line):
        continue
    all_cf_names.append(line)
f.close()


f = open(temp_jmx_query_file, 'w')
f.write('open ' + CASSANDRA_PID + '\n')
for cf in all_cf_names:
    if(len(cf) > pad_length):
        pad_length = (len(cf)/5)*5 + 5
    f.write('bean org.apache.cassandra.metrics:type=ColumnFamily,keyspace=' + KEYSPACE_NAME + ',scope='+ cf +',name='+ metric_type + '\n')
    f.write('get ' + metric_name + '\n')
f.write('exit' + '\n')
f.close()


f = open(temp_jmx_query_file, 'r')
p = subprocess.Popen(['java','-jar', JMX_TERM_JAR], stdout=subprocess.PIPE,stdin=subprocess.PIPE,stderr=subprocess.PIPE)
for line in f.readlines():
    p.stdin.write(line)
jmx_output = p.stdout.read()
p.stdin.close()
f.close()

jmx_output = filter(None,jmx_output.split("\n"))
if(len(jmx_output) != len(all_cf_names)):
    print "There was a problem fetching metrics"
    sys.exit(0)

final_cf_output = []

for i in range(len(all_cf_names)):
    jmx_output[i] = jmx_output[i][:-1].split(" = ")
    final_cf_output.append([all_cf_names[i], float(jmx_output[i][1])])

final_cf_output = sorted(final_cf_output, key=itemgetter(1), reverse=True)[:top_number]

for cf in final_cf_output:
    print cf[0].ljust(pad_length), cf[1]
