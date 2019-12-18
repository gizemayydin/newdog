import json
import glob
import sys

def mangle(s):
    return s.strip()[1:-1]

def cat_json(output_filename, input_filenames):
    with open(output_filename, "w") as outfile:
        first = True
        for infile_name in input_filenames:
            with open(infile_name) as infile:
                if first:
                    outfile.write('[')
                    first = False
                else:
                    outfile.write(',')
                outfile.write(mangle(infile.read()))
        outfile.write(']')



if __name__ == '__main__':
    output_filename = str(sys.argv[1])
    path_to_rawdocs = str(sys.argv[2])

    filenames = glob.glob(path_to_rawdocs+"/*.json")

    input_filenames = []
    for file in filenames:
        input_filenames.append(file)

    cat_json(output_filename,input_filenames)

