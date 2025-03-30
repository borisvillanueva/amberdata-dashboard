# amberdata-rest
Amberdata Python Rest Service

This is a Python package for accessing Amberdata's REST API.

The purpose of this package is to provide a simple abstraction layer for interacting with Amberdata's REST API.
We'll aim to keep this package as up to date as possible with the latest features and endpoints available from Amberdata.


This package currently supports:
- Spot REST API

## Development

### Installation
1) Install python 3.10 or higher (previous versions may work as well but please note that they are not officially supported)
2) Install pipenv with the command: `pip install pipenv`
2) Navigate to the project root folder and run install dependencies with the command: `pipenv install`

### Usage
1) Create a local file with your API Key similar to the following format:
{
    "amberdata_api_key":  "PLACE YOUR KEY HERE"
}
2) Instantiate the service you need with the filepath to the API Key file
3) You are now ready to use the service and interact with Amberdata's REST API

### Examples
 Each package in this repo will have an example.py that provides a few simple examples of how to use the package.


### Run tests
Run test with coverage to report on code coverage:
1) Install development dependencies with the command: `pipenv install --dev`
2) Run the tests with the following commands to get coverage reports:
```coverage run -m pytest```
```coverage report```