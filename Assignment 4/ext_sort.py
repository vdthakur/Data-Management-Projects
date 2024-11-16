import csv
import os

def sort_and_write_chunk(chunk, run_id):
    """
    Sorts a chunk of data in-memory by the first column and writes it to a temporary file.
    
    :param chunk: A list of data rows to be sorted.
    :param run_id: Identifier for the run, used for naming the temporary file.
    """
    
    sorted_chunk = sorted(chunk, key=lambda x: x[0])
    tempFilename = f"chunk{run_id}.csv"

    with open(tempFilename, 'w', newline='') as tempFile:
        chunkWriter = csv.writer(tempFile)
        chunkWriter.writerows(sorted_chunk)
        
    print(f"Chunk sorted and written to {tempFilename}: {sorted_chunk}")

    return tempFilename


def merge_runs(run_files, output_filename):
    """
    Merges sorted files (runs) into a single sorted output file.
    
    :param run_files: List of filenames representing sorted runs to be merged.
    :param output_filename: Filename for the merged, sorted output.
    """
    
    roundMerge = 1  

    while len(run_files) > 1:
        carryOverFiles = []
        for i in range(0, len(run_files), 2):
            # run for as long as the run ID of the second file is within amount of total files
            if i + 1 < len(run_files):
                tempFile = f"temp_merge_{roundMerge}_{i//2}.csv"

                with open(run_files[i], "r") as file1, open(run_files[i+1], "r") as file2, open(tempFile, "w", newline='') as fileOutput:
                    reader1 = csv.reader(file1)
                    reader2 = csv.reader(file2)
                    writer = csv.writer(fileOutput)
                    # ensures only two rows is brought into main memory from the two run files
                    row1 = next(reader1, None)
                    row2 = next(reader2, None)

                    while row1 or row2:
                        if row2 is None or (row1 and row1[0] <= row2[0]):
                            writer.writerow(row1)
                            row1 = next(reader1, None)
                        else:
                            writer.writerow(row2)
                            row2 = next(reader2, None)

                carryOverFiles.append(tempFile)
            else:
                # since there may be odd number of run files, need to carry over if they were not paired
                carryOverFiles.append(run_files[i])

        for file in run_files:
            if file not in carryOverFiles:  
                os.remove(file)
        # reinitate run files with new run files that were carried over
        run_files = carryOverFiles  
        roundMerge += 1

   
    if run_files and run_files[0] != output_filename:
        os.rename(run_files[0], output_filename)
    
    print(f"File merged in {roundMerge} rounds")
    print(f"Final sorted file is:", output_filename)


def external_sort(input_filename, output_filename):
    """
    the external sort process: chunking, sorting, and merging.

    :param input_filename: Name of the file with data to sort.
    :param output_filename: Name of the file where sorted data will be written.
    """
    run_id = 1
    chunkSize = 2  
    run_files = []
    chunk = []

    with open(input_filename,'r') as infile:
        reader = csv.reader(infile)
        
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunkSize:
                fileName = sort_and_write_chunk(chunk,run_id)
                run_files.append(fileName)
                run_id += 1
                chunk = []  

        # account for the fact that one chunk may just have one row
        if chunk:
            fileName = sort_and_write_chunk(chunk,run_id)
            run_files.append(fileName)
    
    merge_runs(run_files,output_filename)
       


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python3 ext_sort.py input.csv output.csv")
    else:
        input_filename = sys.argv[1]
        output_filename = sys.argv[2]
        external_sort(input_filename, output_filename)