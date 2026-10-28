import sys
import json
import requests

def print_response(response):
    try:
        print(json.dumps(response.json(), indent=2))
    except:
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")

if len(sys.argv) < 2:
    print("Usage:")
    print("  INGEST <file> <gateway>")
    print("  QUERY <gateway> <query_type> <value>")
    print("  PURGE <gateway>")
    sys.exit(1)

command = sys.argv[1]

if command == "INGEST":
    if len(sys.argv) < 4:
        print("Usage: INGEST <file> <gateway>")
        sys.exit(1)
    
    file_path = sys.argv[2]
    gateway = sys.argv[3]
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        response = requests.post(
            f'http://{gateway}/ingest',
            json={'content': content},
            timeout=10
        )
        print_response(response)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
    except Exception as e:
        print(f"Error: {e}")

elif command == "QUERY":
    if len(sys.argv) < 5:
        print("Usage: QUERY <gateway> <query_type> <value>")
        print("Query types: SEARCH_HOST, SEARCH_DAEMON, SEARCH_SEVERITY, SEARCH_KEYWORD, COUNT_KEYWORD, SEARCH_DATE")
        sys.exit(1)
    
    gateway = sys.argv[2]
    query_type = sys.argv[3]
    value = sys.argv[4]
    
    try:
        response = requests.get(
            f'http://{gateway}/query',
            params={'type': query_type, 'value': value},
            timeout=10
        )
        print_response(response)
    except Exception as e:
        print(f"Error: {e}")

elif command == "PURGE":
    if len(sys.argv) < 3:
        print("Usage: PURGE <gateway>")
        sys.exit(1)
    
    gateway = sys.argv[2]
    
    try:
        response = requests.delete(
            f'http://{gateway}/purge',
            timeout=10
        )
        print_response(response)
    except Exception as e:
        print(f"Error: {e}")

else:
    print(f"Unknown command: {command}")
    print("Available: INGEST, QUERY, PURGE")
