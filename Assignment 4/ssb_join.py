import csv


def simple_sort_based_join(product_filename, maker_filename, output_filename):
    """
    Executes a simple sort-based join between two datasets and writes the output to a CSV file.

    :param product_filename: Filename of the sorted product dataset.
    :param maker_filename: Filename of the sorted maker dataset.
    :param output_filename: Filename for the output joined dataset.
    """
    # 2 rows is 1 page, each buffer is max 1 page
    bufferSize = 2  

    # open both files for reading
    with open(product_filename,'r') as productFile, open(maker_filename,'r') as makerFile:
        product = csv.reader(productFile)
        maker = csv.reader(makerFile)

        # initialize buffers with the first batch of rows
        productBuffer = [next(product, None) for num in range(bufferSize)]
        makerBuffer = [next(maker, None) for num in range(bufferSize)]
        productBuffer = [row for row in productBuffer if row]  
        makerBuffer = [row for row in makerBuffer if row]

        with open(output_filename,'w',newline='') as output:
            writer = csv.writer(output)

            while productBuffer or makerBuffer:

                if productBuffer[0][0] == makerBuffer[0][0]:
                    writer.writerow([productBuffer[0][0], productBuffer[0][1], makerBuffer[0][1]])
                    productBuffer = productBuffer[1:]
                    makerBuffer = makerBuffer[1:]
                elif productBuffer[0][0] < makerBuffer[0][0]:
                    productBuffer = productBuffer[1:]
                else:
                    makerBuffer = makerBuffer[1:]

                if len(productBuffer) < bufferSize and (nextRow := next(product, None)) is not None:
                    productBuffer.append(nextRow)
                if len(makerBuffer) < bufferSize and (nextRow := next(maker, None)) is not None:
                    makerBuffer.append(nextRow)

    print("Joined file created as",output_filename)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python ssb_join.py <product_file.csv> <maker_file.csv> <output_file.csv>")
    else:
        simple_sort_based_join(sys.argv[1], sys.argv[2], sys.argv[3])
