import random
import csv
import argparse
# open a csv file without loading it all in memory
# sample n lines of the csv file without duplicates and writes them to a new csv file
# uses commandline arguments for input file and output file

parser = argparse.ArgumentParser()

parser.add_argument("input_file", help="input file")

parser.add_argument("output_file", help="output file")

parser.add_argument("n", help="number of lines to sample")

args = parser.parse_args()

# sample large csv file without loading the entire file into memory

# open the input file


def sample_large_csv(input_file, output_file, n):
    with open(input_file, "r") as f:
        results = []
        reader = csv.reader(f)
        for _ in range(n):
            results.append(next(reader))
        random.shuffle(results)
        reader = csv.reader(f)
        for i, v in enumerate(reader, n):
            r = random.randint(0, i)
            if r < n:
                results[r] = v 
        with open(output_file, "w") as f2:
            writer = csv.writer(f2)
            writer.writerows(results)


sample_large_csv(args.input_file, args.output_file, int(args.n))
