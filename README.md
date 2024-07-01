# DataToCare
Code Repository of DatatoCare Treatment Recommendation Pipeline.  

To run DatatoCare Code, see the instructions below.

## Overview
This repository contains the code necessary to run the DataToCare treatment prediction pipeline. 
The patient's current medical state and relevant medical history of former patients are used to identify treatments that should be administered in ICU. 
DataToCare is built upon real-world clinical dataset [MIMIC-III](https://mimic.physionet.org/).

## Requirements
- PostgreSQL 11
- Python 3.8
- UMAP 0.51
- scikit-learn 0.24
- multiprocessing default
- pandas 1.2
- numpy 1.19

## Code Execution

### Setup
Running setup will preprocess the data which involves filtering patients to only those that have only MetaVision data, standardizing the drug data, and determining times for specific diagnosis by examining nurse notes.
To setup:
1) download and build mimiciii with PostgreSQL (https://github.com/MIT-LCP/mimic-code/tree/main/mimic-iii/buildmimic/postgres)
2) Edit parameters in **./common/databse_connection_parameters.txt** to ensure a connection with the mimiciii-database built in step 1. 
2) run script **./setup/main.py**

### Experiments
In the experiments folder, you will find two sub-folders, one containing the tuning experiments corresponding to four strategies as described in section 4 and the other containing the testing experiments as described in section 5.

