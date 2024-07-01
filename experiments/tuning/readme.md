## Tuning Experiments Explanation
In this folder, you will find sub-folders that contain code to run a specific strategy evaluating corresponding tuning experiments which are described in section 4 of the paper. 

Here is a brief description of each experiment, detailed explanation could be found in the paper:  
1. * **Abnormal-hot-accumulated (AHA)** :  
In this tuning experiment, we evaluate the strategy of abnormal-
hot-accumulated which for encoding variant uses Abnormality-Hot Encoding, for abnormals uses accumulated one's and for relevant patient selection method uses similarity.
2. * **Abnormal-umap-instantaneous (AUI)** :  
In this tuning experiment, we evaluate the strategy of abnormal-umap-instantaneous which for encoding variant uses Reduced dimensionality, for abnormals uses instantaneous one's and for relevant patient selection method uses similarity.
3. * **Uniform-umap-accumulated (UUA)** :  
In this experiment, we evaluate the strategy of uniform-umap-accumulated which for encoding variant uses Reduced dimensionality, for abnormals uses accumulated one's and for relevant patient selection method uses uniform choice.  
4. * **Abnormal-umap-accumulated (AUA)** :  
In this experiment, we evaluate the strategy of abnormal-umap-accumulated which for encoding variant uses Reduced dimensionality, for abnormals uses accumulated one's and for relevant patient selection method uses similarity.  . 


In each of the experiment subfolders, you will find scripts (with code variations pertaining to the experiment variant) and csv files that will first make features vector and then using that to make training data which will eventually be used to make models for predicting treatments to be given to input patients (300 tuning patients).

Files listed in the experiment sub-folders are scripts: main.py, compute.py, evaluate.py, find_treatments.py, build_state_vectors.py, build_feature_vectors, build_models_predictions.py, helper.py, and csv files: experiment_micu_eval.csv and valid_admissions_wo_holdout.csv.  

Here is the description of each:

* **main.py** :
Main script file that handles the connection to mimiciii-database and calls submodules to execute the variant of the pipeline. 

* **compute.py**:
The script file that using discharge measurement tables computes statistics for numerical using quantiles, all values between the10𝑡ℎ quantile and the90𝑡ℎ quantile as normal; values below the 10𝑡ℎ quantile as low abnormal and values above the 90𝑡ℎ quantile as high abnormal values

* **evaluate.py**:
Script file that using computed statistics first evaluate given patient state and then using patient state and statistics determine all-close patients.

* **find_treatments.py** :
Script file that finds all treatments given to K/all-close patients and returns them as Dataframe. 

* **build_base_vectors.py** :
Script file that first finds all times for which patients have some measurement taken or diagnosis made. Then base vectors are made by incorporating features vectors (demographics, diagnosis, measurements, treatments) with initial values. In last, vectors are partitioned by patients and features type. Partitions are created to enable concurrency through multiprocessing.

* **build_features_vectors.py** :
Script file that has functions that enrich features vectors. Each vectors type (Measurement, Treatment, Demographics, and Diagnosis) is calculated using separate processes for each patient.

* **build_models_predictions.py** :
Script file that processes feature vectors to build a prediction model for each treatment.

* **helper.py** :
Script file that gets and returns measurement type (categorical or numerical) information.

* **cal.py** :
Script file that compiles prediction results and calculates metrics of accuracy with respect to patients and accuracy with respect to treatments.

* **experiment_micu_eval.csv** :
A CSV file that contains patients' information for tuning experiments. subject_id is the unique identifier that specifies an individual patient, hadm_id column is patient admission id and evaltime column is the time we evaluate the patient. admittime is the admission time of the patient and timediff is the time difference in hours between admission and evaluation time.

* **valid_admissions_wo_holdout.csv** :
A CSV file that contains the the non-sequestered patients who are ever admitted to the MICU. Additionally, it contains the age attribute of patients with respect to specific admission.
