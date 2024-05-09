#!/usr/bin/env python3

import os
import zlib
import argparse

def create_deflate_from_lines(input_file, num_lines=100):
    # Read and decompress the original .deflate file
    with open(input_file, 'rb') as f_in:
        compressed_data = f_in.read()
        decompressed_data = zlib.decompress(compressed_data).decode()

    # Get the first `num_lines` lines
    lines = decompressed_data.splitlines()[:num_lines]
    compressed_lines = '\n'.join(lines).encode()

    # Create the output directory if it doesn't exist
    if not os.path.exists('tiny-data'):
        os.makedirs('tiny-data')

    # Define the output file path
    output_file = os.path.join('tiny-data', input_file)

    # Compress and write the first 100 lines to the new .deflate file
    with open(output_file, 'wb') as f_out:
        f_out.write(zlib.compress(compressed_lines))

    print(f"New .deflate file created with the first {num_lines} lines: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a new .deflate file with the first specified lines from the input file")
    parser.add_argument("input_file", help="Path to the original .deflate file")
    parser.add_argument("-n", "--num-lines", type=int, default=100, help="Number of lines to include in the new .deflate file (default: 100)")
    args = parser.parse_args()

    create_deflate_from_lines(args.input_file, args.num_lines)
