import argparse
import glob
from read_ccse_csv_data import read_csv

if __name__ == '__main__':
    def main():

        parser = argparse.ArgumentParser()
        parser.add_argument("file_names", nargs='*')

        args = parser.parse_args()
        file_names = []

        for arg in args.file_names:
            file_names += glob.glob(arg)

        for file in file_names:
            read_csv(file)


    main()
