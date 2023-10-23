# Programming Exercise Using PySpark

This code joins customers dataset and accounts dataset and removes sensitive data to be able to obtain a joint dataset with bitcoin accounts and their corresponding customer details.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

Docker - Install Docker on your local machine
For more information on how to install Docker, checkout: [Docker](https://docs.docker.com/get-docker/)


### Installing and Running

The input files and the country list be modified on the Dockerfile on this line

```
CMD ["python", "main.py", "<data1>" ,"<data>","Country1" ,"Country2",....... "CountryN"] 
```

Once docker is installed and you modify your input parameters go to your current directory and run the ff:

docker build -t <appname> .  

```
docker build -t my-python-app .  
```

After the image has been built run the ff:

docker build -t <appname> . 

```
docker build -t my-python-app . 
```

Output Files can be found on the client data folder.
