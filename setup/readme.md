## Setup Code Explanation
In this folder, you will find scripts and text files that will preprocess the data which involves filtering patients, standardizing the drug data, and determining times for patients' diagnosis.
The setup could take upwards of one day with patients' diagnosis timeline building being the most time-consuming process.

File listed in the folder are scripts: main.py, check_db_setup.py, clean.py,  build_measurement.py, build_diagnosis.py, and standardize.py, text file: regex_cleaning_sql_scripts.txt, and csv file: mapping_inputevents_itemid_parent.csv.

Here is the description of each:

* **main.py**:
Main script file that handles the connection to mimiciii-database and calls submodules in a pipeline fashion to build setup.  

* **check_db_setup.py**:
Script file that checks whether essential tables existed and loaded completely or not.  

* **clean.py**:
Script file that clean and optimize the dataset, if not already, by making new patient and measurement tables (d3sv1_patients_mv, d3sv1_chartevents_mv, and d3sv1_labevents_mv) that have only mv patients data. Also, add an index on new measurement tables so that data can be accessed quickly.  
 
* **build_measurement.py**:
Script file that calculates discharge measurements by making discharge measurement tables (d3sv1_chartevents_mv_dm, and d3sv1_labevents_mv_dm) from measurement tables (new one's created by clean.py) and also identifies/resolve potential clashes between measurements.  

* **build_diagnosis.py** :
Script file that builds patients' diagnosis time table with diagnosis enriched with information (individual diagnoses and their times over patient stay) from noteevents, using matching of diagnosis short_title and noteevent text. Also matched diagnoses (icd9_code) are mapped to higher-level groups and added in patients' diagnosis time table along with accompanying noteevent information.

* **standardize.py** :
Script that performs the mapping and unifies similar drugs based on labels with different ids (itemids). The mappings with base to clean to generic name are stored in a new table of "d3sv1_drugs_mapping".
Before mapping, labels are cleaned (using regex SQL scripts) so that drugs that are similar, with different itemids due to spelling mistakes in a label, can be unified.

* **regex_cleaning_sql_scripts.txt** :
Text file that contains regex cleaning SQL scripts. Each line corresponds to a different script. This file is used by standardize.py.

* **mapping_inputevents_itemid_parent.csv** :
CSV File that contains the mappings from clean to generic drugs labels for enhanced unification.