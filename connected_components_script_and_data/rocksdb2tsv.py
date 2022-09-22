# rocksdb to a tsv file
# where each key and value is stored in a row
from ast import Assert
import csv
import time
import rocksdb
import sys

start = time.time()
class AssocCounter(rocksdb.interfaces.AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        if existing_value:
            s = int(existing_value) + int(value)
            return (True, str(s).encode())
        return (True, value)

    def name(self):
        return b'AssocCounter'

opts = rocksdb.Options()
opts.merge_operator = AssocCounter()
identity_set =  rocksdb.DB(sys.argv[1], opts)
it = identity_set.iteritems()
it.seek_to_first()
with open(f"{sys.argv[1].split('.')[0]}.tsv", 'w') as out:
    tsv = csv.writer(out, delimiter="\t")
    for k,v in it:
        tsv.writerow([k.decode(),v.decode()])

end = time.time()
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
time_formated = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
print("Time taken = ", time_formated)
