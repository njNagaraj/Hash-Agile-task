import requests
import pandas as pd

# Define Solr base URL (make sure this matches your local Solr setup)
SOLR_URL = 'http://localhost:8983/solr/'  # Change this if needed

# Function to get employee count from a collection
def getEmpCount(p_collection_name):
    print(f"Getting employee count from collection {p_collection_name}")
    try:
        response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q=*:*&rows=0")
        response.raise_for_status()
        count = response.json().get('response', {}).get('numFound', 0)
        print(f"Employee count in {p_collection_name}: {count}\n\n")
        return count
    except Exception as e:
        print(f"Failed to get employee count. Error: {e}")

# Function to index data into a collection excluding a specific column
def indexData(p_collection_name, p_exclude_column):
    print(f"Indexing data into collection {p_collection_name}, excluding column: {p_exclude_column}")
    try:
        # Load employee data from CSV file
        employee_data = pd.read_csv('employees.csv', encoding='latin1')
        
        # Drop the excluded column
        if p_exclude_column in employee_data.columns:
            employee_data = employee_data.drop(columns=[p_exclude_column])
        
        # Convert DataFrame to JSON
        json_data = employee_data.to_json(orient='records')
        
        # Prepare the request to Solr
        headers = {'Content-Type': 'application/json'}
        response = requests.post(f"{SOLR_URL}{p_collection_name}/update?commit=true", 
                                 headers=headers, 
                                 data=json_data)  # Send JSON data directly
        
        response.raise_for_status()
        print(f"Successfully indexed data into {p_collection_name}\n\n")
    except Exception as e:
        print(f"Failed to index data. Error: {e}")

# Function to search by a column in a collection
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    print(f"Searching in collection {p_collection_name} where {p_column_name} = {p_column_value}")
    try:
        response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q={p_column_name}:{p_column_value}")
        response.raise_for_status()
        results = response.json().get('response', {}).get('docs', [])
        print(f"Found {len(results)} records in {p_collection_name}:\n\n")
        for result in results:
            print(result)
    except Exception as e:
        print(f"Failed to search data. Error: {e}")

# Function to delete an employee by ID
def delEmpById(p_collection_name, p_employee_id):
    print(f"Deleting employee with ID {p_employee_id} from collection {p_collection_name}")
    try:
        delete_query = f"<delete><id>{p_employee_id}</id></delete>"
        headers = {'Content-Type': 'text/xml'}
        response = requests.post(f"{SOLR_URL}{p_collection_name}/update?commit=true", 
                                 headers=headers, 
                                 data=delete_query)
        response.raise_for_status()
        print(f"Successfully deleted employee ID {p_employee_id} from {p_collection_name}\n\n")
    except Exception as e:
        print(f"Failed to delete employee. Error: {e}")

# Function to get department facet counts
def getDepFacet(p_collection_name):
    print(f"Getting department facet counts from collection {p_collection_name}")
    try:
        response = requests.get(f"{SOLR_URL}{p_collection_name}/select?q=*:*&facet=true&facet.field=Department")
        response.raise_for_status()
        facets = response.json().get('facet_counts', {}).get('facet_fields', {}).get('Department', [])
        print(f"Department facet counts for {p_collection_name}: {facets}\n\n")
    except Exception as e:
        print(f"Failed to get department facet counts. Error: {e}")

# Main execution flow
if __name__ == "__main__":
    print("----- Starting Solr Task Execution -----")

    # Define collection names
    v_nameCollection = 'emp_coll_1'
    v_phoneCollection = 'emp_coll_2'
    
    # Step 1: Getting employee count before indexing
    print(f"Step 2: Getting employee count from collection {v_nameCollection}")
    getEmpCount(v_nameCollection)
    
    # Step 3: Indexing data into collections
    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')
    
    # Step 4: Deleting an employee by ID
    delEmpById(v_nameCollection, 'E02003')
    
    # Step 5: Getting employee count after indexing
    print(f"Step 6: Getting employee count from collection {v_nameCollection}")
    getEmpCount(v_nameCollection)
    
    # Step 7: Searching by columns
    searchByColumn(v_nameCollection, 'Country', 'China')
    searchByColumn(v_nameCollection, 'Gender', 'Male')
    searchByColumn(v_phoneCollection, 'Department', 'IT')
    
    # Step 8: Getting department facet counts
    getDepFacet(v_nameCollection)
    getDepFacet(v_phoneCollection)

    print("----- Solr Task Execution Completed -----")
