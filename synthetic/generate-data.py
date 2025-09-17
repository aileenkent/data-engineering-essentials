from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import pandas as pd
import numpy as np

"""
This script generates 3M rows of synthetic data with missing values and duplicates.
It uses the SDV library to generate the synthetic data based on a Pandas DataFrame,
    populated with random data provided by numpy.
It then adds duplicates and missing values to the data.
It then saves the data to a CSV and a Parquet file.
"""

start_date = pd.Timestamp('1930-01-01')
end_date = pd.Timestamp('2006-12-31')
date_range = (end_date - start_date).days
random_days = np.random.randint(0, date_range, 100)
birth_dates = [start_date + pd.Timedelta(days=days) for days in random_days]

sample_data = pd.DataFrame({
    'id': range(1, 101),  # 100 rows
    'income': np.random.normal(50000, 15000, 100),
    'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], 100),
    'birth_date': birth_dates,  # Random birthdates in YYYY-MM-DD format
    'score': np.random.uniform(0, 100, 100)  # This column will have 20% missing values
})

# Introduce 20% missing values in the 'score' column
missing_indices = np.random.choice(sample_data.index, size=int(0.2 * len(sample_data)), replace=False)
sample_data.loc[missing_indices, 'score'] = np.nan

print(f"Sample data shape: {sample_data.shape}")
print(f"Missing values in 'score' column: {sample_data['score'].isna().sum()} ({sample_data['score'].isna().mean():.1%})")

# Create metadata for the single table
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(sample_data)

# The missing values proportion is automatically detected from the data
# No need to manually set it - SDV will learn the pattern from the training data
print("Metadata created")
print(f"Column names: {metadata.get_column_names()}")

# ------------------------------------------------------------
# Now use the metadata to generate the synthetic data
# ------------------------------------------------------------

# Initialize the GaussianCopulaSynthesizer
synthesizer = GaussianCopulaSynthesizer(metadata)

# Fit the synthesizer to the sample data
synthesizer.fit(sample_data)

# Generate synthetic data
synthetic_data = synthesizer.sample(num_rows=3000000)

print(f"\nSynthetic data shape before adding duplicates: {synthetic_data.shape}")

# Introduce a small number of duplicate rows (about 0.8% of the data)
num_duplicates = int(0.007924 * len(synthetic_data))
print(f"Adding {num_duplicates} duplicate rows...")

# Randomly select rows to duplicate
duplicate_indices = np.random.choice(synthetic_data.index, size=num_duplicates, replace=False)
duplicate_rows = synthetic_data.loc[duplicate_indices].copy()

# Add the duplicate rows to the dataset
synthetic_data_with_duplicates = pd.concat([synthetic_data, duplicate_rows], ignore_index=True)

# Shuffle the data to randomize the position of duplicates
synthetic_data_with_duplicates = synthetic_data_with_duplicates.sample(frac=1, random_state=42).reset_index(drop=True)

print(f"Synthetic data shape after adding duplicates: {synthetic_data_with_duplicates.shape}")
print(f"Missing values in 'score' column: {synthetic_data_with_duplicates['score'].isna().sum()} ({synthetic_data_with_duplicates['score'].isna().mean():.1%})")

# Check for actual duplicates (excluding the id column which should be unique)
duplicate_check = synthetic_data_with_duplicates.drop('id', axis=1).duplicated().sum()
print(f"Number of duplicate rows (excluding id): {duplicate_check}")

print("\nFirst 10 rows of synthetic data:")
print(synthetic_data_with_duplicates.head(10))

print("\nSummary statistics:")
print(synthetic_data_with_duplicates.describe())

# Save the synthetic data to CSV
output_file = 'synthetic_data_with_missing_and_duplicates.csv'
synthetic_data_with_duplicates.to_csv(output_file, index=False)
synthetic_data_with_duplicates.to_parquet("synthdata_with_duplicates.parquet", index=False)
