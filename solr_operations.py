import pysolr

# Function to index employee data excluding a specific column
def index_data(collection_name, exclude_column):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True)
    
    # Sample employee data
    employees = [
        {"id": "E02001", "Name": "Alice", "Department": "IT", "Gender": "Female"},
        {"id": "E02002", "Name": "Bob", "Department": "HR", "Gender": "Male"},
        {"id": "E02003", "Name": "Charlie", "Department": "IT", "Gender": "Male"},
        # Add more employee entries if needed
    ]
    
    # Index the data, excluding the specified column
    indexed_data = [{k: v for k, v in emp.items() if k != exclude_column} for emp in employees]
    solr.add(indexed_data)
    print(f"Indexed data into {collection_name}, excluding column {exclude_column}.")

# Function to search for records by a specified column and value
def search_by_column(collection_name, column_name, column_value):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True)
    results = solr.search(f'{column_name}:{column_value}')
    return results

# Function to get the total count of employees in a collection
def get_emp_count(collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True)
    results = solr.search('*:*')  # Fetch all documents
    return len(results)

# Function to delete an employee by ID
def del_emp_by_id(collection_name, employee_id):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True)
    solr.delete(id=employee_id)
    print(f"Deleted employee with ID {employee_id} from {collection_name}.")

# Function to retrieve employee count grouped by department
def get_dep_facet(collection_name):
    solr = pysolr.Solr(f'http://localhost:8983/solr/{collection_name}', always_commit=True)
    # Perform a search with facet parameters
    results = solr.search('*:*', **{
        'facet': 'true',
        'facet.field': 'Department'
    })
    
    # Return the facet counts
    return results.facets['facet_counts']['facet_fields']['Department']

# Main execution
if __name__ == "__main__":
    collection_name = 'employees'  # Your collection name

    # Ensure the collection exists in Solr
    print(f"Ensure that the '{collection_name}' collection exists in Solr.")

    print("Employee count in collection:", get_emp_count(collection_name))
    index_data(collection_name, 'Department')
    
    del_emp_by_id(collection_name, 'E02003')

    print("Employee count after deletion:", get_emp_count(collection_name))
    print("Search results for Department='IT':", list(search_by_column(collection_name, 'Department', 'IT')))
    print("Search results for Gender='Male':", list(search_by_column(collection_name, 'Gender', 'Male')))
    print("Department facet count:", get_dep_facet(collection_name))
