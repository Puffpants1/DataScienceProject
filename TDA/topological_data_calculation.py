import numpy as np
import gudhi as gd
import os
import time
import gudhi.representations
import csv
from multiprocessing import Pool, cpu_count, Lock

# Global lock for safe writing to files in a multiprocessing environment
lock = Lock()

def load_processed_refcodes():
    """
    Load refcodes

    This function first checks if the times.csv file exists. 
    If it doesn't, this implies that no refcodes have been processed yet, and it returns an empty set.

    If the times.csv file exists, this function opens this file and reads column (which contains the refcodes).
    It then stores these refcodes in a set (for fast lookup) and returns this set.
    Returns:
    - A set of refcodes that have been processed, stored in 'times.csv'.
    """
    if not os.path.exists("times.csv"):
        return set()
    with open("times.csv", "r") as times_file:
        times_reader = csv.reader(times_file)
        processed_refcodes = {row[0] for row in times_reader}
    return processed_refcodes

# Function to process each refcode
def process_refcode(refcode):
    """
    Process a single refcode by performing the following steps:
    1. Load the corresponding coordinates from a CSV file.
    2. Construct a Rips complex and compute its persistence diagram.
    3. Generate and save a persistence landscape.
    4. Record processing results and time taken.

    Parameters:
    - refcode: The reference code for the structure to be processed.

    Returns:
    - A tuple containing the refcode and a status message.
    """
    result = {}
    time_taken = {}

    # Check if the CSV file for the refcode exists
    csv_path = f'./data1/{refcode}.csv'
    if os.path.exists(csv_path):
        print(f'Loading {refcode}')
        
        # Load coordinates from the CSV file
        coordinates = np.genfromtxt(csv_path, delimiter=",")
        print('Now trying Rips')
        try:
            # Start timing
            start = time.time()

            # Construct the Rips complex and compute the persistence diagram
            Rips_complex_sample = gd.RipsComplex(points=coordinates, max_edge_length=0.6)
            Rips_simplex_tree_sample = Rips_complex_sample.create_simplex_tree(max_dimension=3)
            diag_Rips = Rips_simplex_tree_sample.persistence()

            # Record the time taken
            stop = time.time()
            result[refcode] = diag_Rips
            time_taken[refcode] = stop - start

            # Generate the persistence landscape
            LS = gd.representations.Landscape(resolution=100) # resolution 分辨率
            # Return persistence intervals in the specified dimension (here, 1 dimension, a ring structure).
            L1 = LS.fit_transform([Rips_simplex_tree_sample.persistence_intervals_in_dimension(1)])  
            try:
                L2 = LS.fit_transform([Rips_simplex_tree_sample.persistence_intervals_in_dimension(2)])
                L = np.concatenate((L1[0], L2[0]))  # Concatenate the landscapes from dimension 1 and 2
            except Exception:
                L = L1[0]  # If L2 fails, use only L1

            # Save the persistence landscape to a .npy file
            with open(f'{refcode}.npy', 'wb') as f:
                np.save(f, L)

            # Safely write the results and time taken to the respective CSV files
            with lock:
                with open("results.csv", "a", newline='') as results_file:
                    results_writer = csv.writer(results_file)
                    results_writer.writerow([refcode, result[refcode]])
                with open("times.csv", "a", newline='') as times_file:
                    times_writer = csv.writer(times_file)
                    times_writer.writerow([refcode, time_taken[refcode]])
            return refcode, "Success"
        except Exception as e:
            # Handle any errors that occur during processing
            with lock:
                with open("results.csv", "a", newline='') as results_file:
                    results_writer = csv.writer(results_file)
                    results_writer.writerow([refcode, 'Error'])
                with open("times.csv", "a", newline='') as times_file:
                    times_writer = csv.writer(times_file)
                    times_writer.writerow([refcode, 'Error'])
            return refcode, str(e)
    else:
        # If the CSV file does not exist, record this in the CSV files
        with lock:
            with open("results.csv", "a", newline='') as results_file:
                results_writer = csv.writer(results_file)
                results_writer.writerow([refcode, 'xyz missing'])
            with open("times.csv", "a", newline='') as times_file:
                times_writer = csv.writer(times_file)
                times_writer.writerow([refcode, 'NA'])
        return refcode, 'xyz file not found'

def main():
    """
    Main function that orchestrates the processing of refcodes.
    It reads the list of refcodes, filters out already processed ones, 
    and then uses multiprocessing to process the remaining refcodes in parallel.
    """
    # Path to the GCD file containing the list of refcodes
    path_to_GCD = './data1/all_potential_cages_selected.gcd'  # MODIFY THIS IF NECESSARY
    gcd_list = open(path_to_GCD, 'r').read()
    refcodes = gcd_list.split("\n")

    # Load the set of already processed refcodes
    processed_refcodes = load_processed_refcodes()

    # Filter out the refcodes that have already been processed
    refcodes_to_process = [refcode for refcode in refcodes if refcode not in processed_refcodes]

    # Calculate the number of remaining files
    total_files = len(refcodes)
    remaining_files = len(refcodes_to_process)

    print(f"Total files: {total_files}")
    print(f"Processed files: {total_files - remaining_files}")
    print(f"Remaining files: {remaining_files}")

    # Use multiprocessing to process refcodes in parallel
    num_processes = min(cpu_count(), 20)  # Use up to 20 cores
    with Pool(num_processes) as pool:
        results = pool.map(process_refcode, refcodes_to_process)

    # Print the status of each processed refcode
    for refcode, status in results:
        print(f"{refcode}: {status}")

if __name__ == "__main__":
    main()
