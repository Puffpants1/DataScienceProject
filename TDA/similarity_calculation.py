import gudhi as gd
import csv
import pandas as pd
import re

results = pd.read_csv('results.csv', header=None, names=['refcode', 'persistence'])
results = results[results['persistence'] != 'xyz missing'].dropna()

results_dict = dict(zip(results['refcode'], results['persistence']))

# Check and fix incomplete persistence entries in the dictionary
for refcode, persistence in results_dict.items():
    if not persistence.endswith(']'):  # Incomplete entries don't end with a closing bracket
        print(f"{refcode} incomplete")
        find_index = persistence.rfind(', (0, (0')
        results_dict[refcode] = persistence[:find_index] + ')]'

# remove intervals with 'inf' from the persistence strings
def replace_inf_with_empty(s):
    return re.sub(r'\(\d+, \([^\)]*inf[^\)]*\)\), ', '', s)

invalid_refcodes = []  # track refcodes with errors during conversion

# Convert persistence strings into actual Python lists, handling potential errors
for refcode, persistence in results_dict.items():
    try:
        string_to_convert = replace_inf_with_empty(persistence)
        results_dict[refcode] = eval(string_to_convert)  # Evaluate the string to convert it to a list
    except Exception as e:
        print(f"error with {refcode}: {e}")
        invalid_refcodes.append(refcode)

# Function to check if the persistence diagram contains Betti 2 intervals
def check_betti_2(persistence_diagram):
    '''
    Checks whether the persistence diagram contains Betti 2 intervals.
    '''
    return any(betti >= 2 for betti, _ in persistence_diagram)

# Dictionary to store refcodes without Betti 2 intervals
no_betti_2 = {}
with open("no_betti_2.csv", "a", newline='') as csvfile:
    writer = csv.writer(csvfile)
    for refcode, persistence_diagram in results_dict.items():
        if not check_betti_2(persistence_diagram):
            writer.writerow([refcode])
            no_betti_2[refcode] = persistence_diagram

# Function to extract persistence intervals corresponding to a given Betti number
def persistence_to_compare(persistence_diagram, betti):
    return [[birth, death] for b, (birth, death) in persistence_diagram if b == betti]

# List of all structure refcodes
structures = list(results_dict.keys())

# DataFrames to store the bottleneck distances for Betti 1 and Betti 2
heatmap_data_1 = pd.DataFrame(index=structures, columns=structures)
heatmap_data_2 = pd.DataFrame(index=structures, columns=structures)

# Calculate bottleneck distances for Betti 1 and Betti 2
for refcode in structures:
    if refcode not in no_betti_2:  # Skip structures without Betti 2 intervals
        for refcode_to_compare in structures:
            if refcode_to_compare not in no_betti_2:
                print(f"Comparing {refcode} and {refcode_to_compare}")
                heatmap_data_2.at[refcode, refcode_to_compare] = gd.bottleneck_distance(
                    persistence_to_compare(results_dict[refcode], 2),
                    persistence_to_compare(results_dict[refcode_to_compare], 2)
                )
                heatmap_data_1.at[refcode, refcode_to_compare] = gd.bottleneck_distance(
                    persistence_to_compare(results_dict[refcode], 1),
                    persistence_to_compare(results_dict[refcode_to_compare], 1)
                )
