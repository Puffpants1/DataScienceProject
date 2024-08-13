import os
import csv
from ccdc.molecule import Molecule
from ccdc.io import EntryReader, CrystalWriter

gcd_file_path = '.gcd'
output_directory = './'
csd_reader = EntryReader('CSD')
moc_entries = EntryReader(gcd_file_path)

potential_cages = []
total_structures = len(moc_entries)
potential_cage_count = 0

def remove_parenthesis(strings):
    """Removes parenthesis from strings and converts them to floats.  '0.123(4)' -> 0.123"""
    return [float(s.split('(')[0]) if '(' in s else float(s) for s in strings]

def process_structure(entry):
    """Processes a structure entry and writes potential cages to files."""
    global potential_cage_count

    refcode = entry.identifier
    heaviest_component = entry.molecule.heaviest_component
    components = entry.molecule.components
    cage = None
    # Check if the heaviest component contains carbon atoms (i.e., is organic)
    if any(atom.atomic_symbol == 'C' for atom in heaviest_component.atoms):  # Organic part check
        cage = heaviest_component
    else:
        # If the heaviest component is not organic, check other components
        for component in components:
            if component != heaviest_component and any(atom.atomic_symbol == 'C' for atom in component.atoms) and component.is_organometallic:
                cage = component
                break

    if cage and any(atom.is_cyclic for atom in cage.atoms):  # If a potential cage structure is found and has at least one cyclic atom
        potential_cages.append(refcode)
        potential_cage_count += 1
        print(f"This is the {potential_cage_count}th potential structure out of {total_structures}")
        print(f"The structure is: {refcode}")
        entry.crystal.molecule = cage # Set the molecule of the crystal to the cage structure

        # Write the crystal structure to a CIF file
        cif_file_path = os.path.join(output_directory, f'{refcode}.cif')
        with CrystalWriter(cif_file_path) as cryst_writer:
            cryst_writer.write(entry.crystal)

        # Append refcode to the potential cages file
        with open(os.path.join(output_directory, 'all_potential_cages_selected.gcd'), 'a+') as file:
            file.write(f'{refcode}\n')

def extract_coordinates_from_cif(filename):
    """Extracts atomic coordinates from a CIF file and writes them to a CSV file."""
    cif_reader = EntryReader(os.path.join(output_directory, filename))
    # Extract atomic coordinates and remove uncertainties from parentheses
    for cif in cif_reader:
        try:
            if cif.has_3d_structure:
                print(f'Extracting coordinates for {filename}')
                x_coords = remove_parenthesis(cif.attributes['_atom_site_fract_x'])
                y_coords = remove_parenthesis(cif.attributes['_atom_site_fract_y'])
                z_coords = remove_parenthesis(cif.attributes['_atom_site_fract_z'])

                # Write the coordinates to a CSV file
                csv_file_path = os.path.join(output_directory, f"{os.path.splitext(filename)[0]}.csv")
                with open(csv_file_path, "w", newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(zip(x_coords, y_coords, z_coords))
        except RuntimeError:
            print(f'Error when extracting coordinates from {filename}')

# Process each entry in the MOC file
for entry in moc_entries:
    process_structure(entry)

# Extract coordinates from all CIF files in the output directory
for filename in os.listdir(output_directory):
    if filename.endswith(".cif"):
        extract_coordinates_from_cif(filename)
