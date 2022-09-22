import random
import csv
import argparse
import rocksdb
# open a csv file without loading it all in memory
# sample n lines of the csv file without duplicates and writes them to a new csv file
# uses commandline arguments for input file and output file

# nohup ./cc_sample.sh > ../sampler_final_final.nohup
class AppendEntity(rocksdb.interfaces.AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        if existing_value:
            s = existing_value + b' ' + value
            return (True, s)
        return (True, value)

    def name(self):
        return b'AppendEntity'


parser = argparse.ArgumentParser()

parser.add_argument("input_file", help="input file")

parser.add_argument("output_file", help="output file")

parser.add_argument("n", help="number of lines to sample")

parser.add_argument("c", help="cc size to sample from")

parser.add_argument("identity_set", help="identity set rocksdb")

parser.add_argument("mapping_is", help="mapping identity set rocksdb")

args = parser.parse_args()

identity_set = rocksdb.DB(
    args.identity_set, rocksdb.Options(merge_operator=AppendEntity()))
mapping_IS = rocksdb.DB(
    args.mapping_is, rocksdb.Options(create_if_missing=False))


def check_cc_size(l):
    mapping_index = mapping_IS.get("".join(l).encode())
    len_cc = len(identity_set.get(mapping_index).decode().split())
    return len_cc

# sample large csv file without loading the entire file into memory


def sample_large_csv(input_file, output_file, n, c):
    with open(input_file, "r") as f:
        results = []

        reader = csv.reader(f)
        for _ in range(n):
            entity = next(reader)
            CC = check_cc_size(entity)
            if c == 1:
                if CC == 2:
                    results.append(entity)
            elif c == 2:
                if CC > 2 and CC <= 10:
                    results.append(entity)
            elif c == 3:
                if CC > 10:
                    results.append(entity)

        random.shuffle(results)

        reader = csv.reader(f)
        for i, v in enumerate(reader, n):
            r = random.randint(0, i)
            if r < n:
                CC = check_cc_size(v)
                if c == 1:
                    if CC == 2:
                        if len(results) == n:
                            results[r] = v
                        else:
                            results.append(v)
                elif c == 2:
                    if CC > 2 and CC <= 10:
                        if len(results) == n:
                            results[r] = v
                        else:
                            results.append(v)
                elif c == 3:
                    if CC > 10:
                        if len(results) == n:
                            results[r] = v
                        else:
                            results.append(v)
        with open(f"{output_file}", "w") as f2:
            writer = csv.writer(f2)
            writer.writerows(results)


sample_large_csv(args.input_file, args.output_file, int(args.n), int(args.c))