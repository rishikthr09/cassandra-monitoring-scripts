import sys

statfile = open(sys.argv[1])

def filter_data(data):
    if(len(sys.argv) == 2):
        return data
    else:
        if sys.argv[2].lower() in data.lower():
            return data
        else:
            return None

count = 0
tables_data = {}
table_name = ""
for line in statfile.readlines():
    line = line[:-1].strip()
    table_data = {}
    if "Table:" in line:
        table_name = line.split()[1]
        tables_data[table_name] = []
    if table_name is not "":
        finaldata = filter_data(line)
        if(finaldata is not None):
            tables_data[table_name].append(line)
    if "Maximum tombstones per slice" in line:
        table_name = ""

collected_tuples = []
for table in tables_data:
    if(len(tables_data[table]) == 0):
        print "No values found"
        break
    if(len(tables_data[table]) > 1 and len(sys.argv) > 2):
        print "Please provide exact string as argument"
        break
    if(len(sys.argv) == 3):
        print table
        for data in tables_data[table]:
            print data
        print
        print
    elif(len(sys.argv) >= 4):
        tup = []
        if("ms" in tables_data[table][0].strip().split()[-1]):
            if(float(tables_data[table][0].strip().split()[-2]) > float(sys.argv[3]) and "NaN" not in tables_data[table][0].strip().split()[-2]):
                tup.append(table)
                tup.append(tables_data[table][0])
        else:
            if(float(tables_data[table][0].strip().split()[-1]) > float(sys.argv[3]) and "NaN" not in tables_data[table][0].strip().split()[-1]):
                tup.append(table)
                tup.append(tables_data[table][0])
        if(len(tup) > 0):
            collected_tuples.append(tup)

for tup in collected_tuples:
    print tup[0] + " --- " + tup[1]
