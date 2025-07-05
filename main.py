import pandas as pd

from product_deduplication import (
    writeToCSV, dataProcessing, identifiesSimilarProducts,
    singleRepresentativeProduct, mergeAllProducts
)


# Read the .parquet file; the path syntax must be written with double "/" or with r (from raw) in front.
initialDataFrame = pd.read_parquet(r"C:\Users\Andrei\Desktop\product_deduplication_python\veridion_product_deduplication_challenge.snappy.parquet")

# If we want to test with a certain number of the first rows
#initialDataFrame = initialDataFrame.head(200)


# Display basic information about the DataFrame, the number of rows and columns
print(f'''\n✅ Loaded parquet file successfully. It contains {len(initialDataFrame)} rows and {initialDataFrame.shape[1]} columns.
      
We want to consolidate duplicates into a single, enriched entry per product,
maximizing available information while ensuring uniqueness.

💡 For all this we will perform the following steps:

1) Writing the data in the CSV file, it will be a good support in making
other files to better visualize each step.

2) Data processing, to perform the analysis in the most efficient way.

3) Identifies similar products based on combined text using fuzzy matching
and saves duplicate groups in CSV and JSON files for later deduplication.

4) Combine the information from each group of duplicates into a single
representative product, keeping the unique values ​​in each column.

5) Merge the consolidated products with the unique ones (which were not part
of any duplicate group) to get a final cleaned and deduplicated set.
''')



# Ask user for confirmation to continue
answer = input(f"💡 We are about to start the product deduplication.\n"
               f"Press 'y' to continue or any other key to cancel: ")



if answer.lower() == "y":

    writeToCSV(initialDataFrame, "1initial_data.csv") # Step 1
    
    dataProcessing("1initial_data.csv", "2preprocessed_data.csv") # Step 2
    
    identifiesSimilarProducts("2preprocessed_data.csv", "3potential_duplicates.csv", "3duplicate_groups.json", 99, 20) # Step 3
    
    singleRepresentativeProduct("2preprocessed_data.csv", "3duplicate_groups.json", "4consolidated_products.csv") # Step 4
    
    mergeAllProducts("2preprocessed_data.csv", "4consolidated_products.csv", "3duplicate_groups.json", "5final_dataset.csv") # Step 5

else:
    print("❌ Operation cancelled.")
    
