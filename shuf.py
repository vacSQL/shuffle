#!/usr/bin/env python3

import random
import os
from tempfile import TemporaryDirectory
import argparse
import sys
import threading
import time

def show_status(chunk_files, is_running):
    """
    Displays the current status of the program when the user presses a key.

    Args:
        chunk_files (list): List of chunk file paths.
        is_running (list): A flag to control the status thread.
    """
    while is_running[0]:
        input("Press Enter to show program status...")
        print(f"\nCurrent status: {len(chunk_files)} chunks remaining.")
        print(f"Chunks processed so far: {chunk_files}\n")

def split_into_chunks(input_file, temp_dir, chunk_size):
    """
    Splits the input file into smaller chunks and saves them to the temporary directory.

    Args:
        input_file (str): Path to the input file.
        temp_dir (str): Path to the temporary directory.
        chunk_size (int): Number of lines per chunk.

    Returns:
        list: List of chunk file paths.
    """
    chunk_files = []
    chunk = []

    with open(input_file, 'r') as f:
        for i, line in enumerate(f):
            chunk.append(line)
            if (i + 1) % chunk_size == 0:  # Write chunks of size `chunk_size`
                random.shuffle(chunk)
                chunk_file_path = os.path.join(temp_dir, f'chunk_{len(chunk_files)}.txt')
                with open(chunk_file_path, 'w') as chunk_file:
                    chunk_file.writelines(chunk)
                chunk_files.append(chunk_file_path)
                chunk = []  # Reset the chunk

        # Write the last chunk if it's not empty
        if chunk:
            random.shuffle(chunk)
            chunk_file_path = os.path.join(temp_dir, f'chunk_{len(chunk_files)}.txt')
            with open(chunk_file_path, 'w') as chunk_file:
                chunk_file.writelines(chunk)
            chunk_files.append(chunk_file_path)

    return chunk_files

def merge_chunks(chunk_files, output_file):
    """
    Merges the shuffled chunks into the output file in random order.

    Args:
        chunk_files (list): List of chunk file paths.
        output_file (str): Path to the output file.
    """
    with open(output_file, 'w') as outfile:
        while chunk_files:  # Continue until all chunks are merged
            # Randomly select a chunk
            selected_chunk = random.choice(chunk_files)
            with open(selected_chunk, 'r') as infile:
                outfile.writelines(infile.readlines())
            # Remove the merged chunk from the list
            chunk_files.remove(selected_chunk)

def shuffle_large_file(input_file, output_file, chunk_size=1000000):
    """
    Shuffles a large file by splitting it into smaller chunks, shuffling each chunk,
    and then merging them in random order into a final output file.

    Args:
        input_file (str): Path to the input file.
        output_file (str): Path to the output file.
        chunk_size (int): Number of lines per chunk. Default is 1,000,000.
    """
    print(f"Starting to shuffle the file: {input_file}")
    print("Press Enter at any time to show program status.\n")

    # Use a temporary directory to store chunks
    with TemporaryDirectory() as temp_dir:
        is_running = [True]  # Flag to control the status thread

        # Start the status thread
        status_thread = threading.Thread(target=show_status, args=([], is_running))
        status_thread.daemon = True  # Daemonize thread to exit when the main program exits
        status_thread.start()

        try:
            # Step 1: Split the input file into chunks
            chunk_files = split_into_chunks(input_file, temp_dir, chunk_size)

            # Step 2: Shuffle the order of the chunk files for better randomness
            random.shuffle(chunk_files)

            # Step 3: Merge the shuffled chunks into the output file in random order
            merge_chunks(chunk_files, output_file)

            print(f"\nFinished! Output file saved to: {output_file}")

        except FileNotFoundError:
            print(f"Error: The input file '{input_file}' does not exist.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An error occurred: {e}", file=sys.stderr)
            sys.exit(1)
        finally:
            is_running[0] = False  # Stop the status thread

def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Shuffle a large file by splitting it into smaller chunks.")
    parser.add_argument('input_file', type=str, help="Path to the input file.")
    parser.add_argument('output_file', type=str, help="Path to the output file.")
    parser.add_argument('--chunk_size', type=int, default=1000000,
                        help="Number of lines per chunk. Default is 1,000,000.")
    parser.add_argument('--version', action='version', version='%(prog)s 1.0',
                        help="Show the version and exit.")

    # Parse arguments
    args = parser.parse_args()

    # Call the shuffle function
    shuffle_large_file(args.input_file, args.output_file, args.chunk_size)

if __name__ == "__main__":
    main()
