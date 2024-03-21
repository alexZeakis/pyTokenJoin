#!/bin/bash

# Check if three arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <token> <endpoint_url> <id>"
    exit 2
fi

# Assign command-line arguments to variables
token="$1"
endpoint_url="$2"
id="$3"

# Construct the first curl command with dynamic arguments and store the response in a variable
response=$(curl -s -X GET -H "Api-Token: $token" "$endpoint_url/api/v1/task/execution/input_json?id=$id")

echo "$response"

# Check if the curl request was successful
success=$(echo "$response" | jq -r '.success')
if [ "$success" != "true" ]; then
    echo "Curl request failed"
    exit 3
fi

# Extract the "result" part from the JSON response
result=$(echo "$response" | jq '.result')

# Store the "result" part into a file named "input.json"
echo "$result" > input.json


# Execute the tool
python main.py input.json output.json

# Perform the second curl request with replacements and store the response in a variable
output_json=$(<output.json)  # Read content of output.json file into a variable

echo "$output_json"

# Replace task_exec_id and output_json with variables
response=$(curl -s -X POST -H "Content-Type: application/json" -H "Api-Token: $token" "$endpoint_url/api/v1/task/execution/output_json" -d "{\"task_exec_id\": \"$id\", \"output_json\": $output_json}")

echo "$response"

# Check if the second curl request was successful
success=$(echo "$response" | jq -r '.success')
if [ "$success" != "true" ]; then
    echo "Second curl request failed"
    exit 4
fi

echo "Second curl request successful"
