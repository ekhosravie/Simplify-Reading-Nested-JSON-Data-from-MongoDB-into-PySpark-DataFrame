Scenario:
Welcome to another exciting tutorial on our channel! Today, we're diving deep into the world of PySpark, where we'll explore how to simplify the process of reading nested JSON data from MongoDB into a PySpark DataFrame. Handling nested structures can be tricky, but fear not—we've got you covered. By the end of this video, you'll be equipped with the knowledge to efficiently work with complex JSON data using PySpark.

Solution:
In this tutorial, we'll start by examining a sample JSON document with nested structures. Understanding its complexity, we'll introduce PySpark, a powerful tool for distributed data processing, and demonstrate how it makes handling nested JSON data a breeze.

We'll walk through a step-by-step solution, showcasing a PySpark function tailored to read nested JSON data from MongoDB. The function takes care of the intricacies, allowing you to select specific columns and create a PySpark DataFrame effortlessly.

Throughout the video, we'll explain the code, highlighting key elements like the SparkSession setup, JSON data reading, and column selection. A real-world example will illustrate the function's application, making it easy for you to adapt it to your own projects.

By the end of the tutorial, you'll have a simplified approach to extract valuable information from nested JSON structures in MongoDB using PySpark. Don't miss out on streamlining your workflow and enhancing your data processing capabilities. Let's jump into the code and simplify reading nested JSON data with PySpark!




# Import necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sample_json_data = '''
{
  "_id": 1,
  "name": "John",
  "age": 30,
  "address": {
    "city": "New York",
    "zip": "10001"
  },
  "contacts": {
    "email": "john@example.com",
    "phone": {
      "home": {
        "landline": "555-1234",
        "mobile": {
          "personal": "555-5678",
          "work": "555-8765"
        }
      },
      "work": "555-5678"
    }
  }
}
'''

# Define a function to read nested JSON data from MongoDB into a PySpark DataFrame
def read_nested_json_from_mongodb(existing_df, nested_struct):
    # Initialize Spark session
    spark = SparkSession.builder.appName("MongoDBReader").getOrCreate()

    # Read data from MongoDB into DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([sample_json_data]))

    # Select the necessary columns from the DataFrame
    selected_cols = [col(nested_col_path).alias(nested_col) for nested_col, nested_col_path in nested_struct.items()]

    result_df = df.select(*selected_cols)

    return result_df

# Example usage:
# Assuming you have an existing DataFrame named 'my_existing_dataframe'
# and want to extract "city" from "address" and "home" from "phone" under "contacts"
existing_dataframe = sample_json_data  # Replace 'my_existing_dataframe' with your actual DataFrame
nested_structure = {"ID": "_id", "age": "age", "zip" : "address.zip" , "city": "address.city", "email": "contacts.email", "home_landline": "contacts.phone.home.landline", "personal_mobile": "contacts.phone.home.mobile.personal"}

result_dataframe = read_nested_json_from_mongodb(existing_dataframe, nested_structure)

# Show the result DataFrame
result_dataframe.show(truncate=False)



