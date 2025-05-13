import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
import json
from collections import defaultdict


# Function for the step 1
def writeToCSV(dataframe: pd.DataFrame, filename: str):
    dataframe.to_csv(filename)
    print(f"✅ Data saved to {filename}.")



# Function for the step 2
def dataProcessing(input_file: str, output_file: str):
    df = pd.read_csv(input_file)

    # Remove whitespace in string columns
    '''Remove whitespace from all string columns to identify duplicates such as "abc" vs "abc".'''
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()


    # Normalize selected text columns to lowercase and remove extra spaces
    '''
    We have selected relevant columns for product identification and comparison.
    Normalize lowercase text to standardize text and identify duplicates like "Abc" vs "abc".
    '''
    text_columns = [
        "product_title", "product_name", "brand", "product_summary",
        "materials", "ingredients", "description"
    ]
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().str.strip()


    # Drop exact duplicates
    '''Drop exact duplicates here, after normalization and whitespace removal.'''
    df = df.drop_duplicates()

    
    # Fill missing values with empty strings for relevant text fields
    '''
    Replace missing values ​​in relevant text fields with empty strings, to prevent
    conversion errors and to enable correct concatenation of subsequent text.
    '''
    text_fields = [
        "product_title", "product_name", "brand", "product_summary",
        "materials", "ingredients", "description", "intended_industries",
        "applicability", "quality_standards_and_certifications", "miscellaneous_features"
    ]
    for col in text_fields:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)


    # Create combined_text field for fuzzy matching
    def combine_text_fields(row):
        parts = [
            row.get("product_title", ""),
            row.get("brand", ""),
            row.get("form", ""),
            row.get("color", ""),
            row.get("materials", "")
        ]
        return " ".join([str(p) for p in parts]).strip()

    df["combined_text"] = df.apply(combine_text_fields, axis=1)

    # Save the preprocessed data
    df.to_csv(output_file, index=False)
    print(f"✅ Preprocessed and cleaned data has been saved to {output_file}.")



# Function for the step 3
def identifiesSimilarProducts(input_file: str, duplicates_csv: str, groups_json: str, similarity_threshold: int, top_k_matches: int):
    df = pd.read_csv(input_file)
    combined_texts = df["combined_text"].tolist()

    # Pre-filter by unspsc
    df["group_key"] = df["unspsc"].fillna("").str.lower().str.strip()
    grouped_indices = defaultdict(list)

    for idx, group in enumerate(df["group_key"]):
        grouped_indices[group].append(idx)
        '''dictionary where the key is the unspsc and the value is the list of indices that belong to that unspsc.'''

    duplicate_groups = []
    visited = set()

    for group, indices in grouped_indices.items():
        if len(indices) <= 1:
            continue
        '''It goes through each group, and if the group has only one product, continue'''

        texts = [combined_texts[i] for i in indices]
        '''Prepare the texts in that group for comparison.'''

        for i, idx_i in enumerate(indices):
            if idx_i in visited:
                '''It compares each product to the others, but skips over those already found previously in other comparisons.'''
                continue

            matches = process.extract(
                combined_texts[idx_i],
                texts,
                scorer=fuzz.token_sort_ratio,
                limit=top_k_matches
            )
            '''Find the closest texts in the list to combined_texts[idx_i], using fuzzy matching.'''

            match_group = [idx_i]
            for match_text, score, j in matches:
                idx_j = indices[j]
                if idx_j != idx_i and score >= similarity_threshold and idx_j not in visited:
                    match_group.append(idx_j)
                    visited.add(idx_j)
            '''Create a list of products similar to idx_i, adding to the match_group only those that score high enough and have not already been processed.'''

            if len(match_group) > 1:
                duplicate_groups.append(match_group)
                visited.update(match_group)
                '''If the group has at least 2 products, it adds it to the final list and marks all products in it as visited.'''

        # If we want to print the processing of this step
        #print(f"🔍 Processed group '{group}' with {len(indices)} products...")

    # Save results
    all_duplicates = [df.loc[i] for group in duplicate_groups for i in group]
    pd.DataFrame(all_duplicates).to_csv(duplicates_csv, index=False)
    print(f"✅ Saved {len(all_duplicates)} duplicates to {duplicates_csv}")

    with open(groups_json, "w") as f:
        json.dump(duplicate_groups, f)
    print(f"✅ Saved {len(duplicate_groups)} groups to {groups_json}")




# Function for the step 4
def singleRepresentativeProduct(input_file: str, groups_file: str, output_file: str):

    # Load preprocessed data
    df = pd.read_csv(input_file)

    # Load the duplicate groups
    with open(groups_file, "r") as f:
        duplicate_groups = json.load(f)


    consolidated_products = []


    # Consolidate each group into one product
    for group in duplicate_groups:
        group_df = df.loc[group]
        consolidated = group_df.iloc[0].copy()

        for col in df.columns:
            if col == "combined_text":
                continue  # Skip combined_text column
            unique_values = group_df[col].dropna().unique()
            consolidated[col] = "; ".join(map(str, unique_values))

        consolidated_products.append(consolidated)

    consolidated_df = pd.DataFrame(consolidated_products)
    consolidated_df.to_csv(output_file, index=False)
    '''
    Iterate through each group of duplicate products.
    It takes the first product in that group and uses it as the basis for the consolidated product.
    For each column, combine the values ​​into a single text string, separated by ;.
    Add the consolidated product to the consolidated_products list.
    '''

    print(f"✅ Saved {len(consolidated_df)} consolidated products to {output_file}.")



# Function for the step 5
def mergeAllProducts(input_file: str, consolidated_file: str, groups_file: str, output_file_csv: str):

    # Load preprocessed data
    full_df = pd.read_csv(input_file)

    # Load the consolidated products
    consolidated_df = pd.read_csv(consolidated_file)

    # Load the duplicate groups
    with open(groups_file, "r") as f:
        duplicate_groups = json.load(f)

    # Collect all duplicate indices into a set
    duplicate_indices = set(idx for group in duplicate_groups for idx in group)

    # Drop the duplicate rows from original data
    unique_rows = full_df.drop(index=duplicate_indices, errors="ignore")

    # Combine with consolidated products
    final_df = pd.concat([unique_rows, consolidated_df], ignore_index=True)

    # Save the final dataset
    final_df.to_csv(output_file_csv, index=False)

    print(f"✅ Final dataset created and saved as {output_file_csv}. It contains {len(final_df)} rows.")

