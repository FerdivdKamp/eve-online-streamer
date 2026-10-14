# eve Online Market Price Notifier

## Introduction
Project is a case for a job application.

The goal is to have the plan and start for a robust streaming data pipeline that serves the purpose of signalling when an event matches specific conditions. 

The condition in question is a 30% dip in market price for an item against a 7-day rolling average.


## Deliverables Criteria

* high-level design of an application that enables data to be processed in a streaming fashion
* show how proficient you are with applying software engineering best-practices while implementing this application
* essential that you can communicate results and convince the client that high-quality work has been delivered
* short presentation of your solution, convince both engineering peers and oblivious clients of the quality of your solution

## Requirements
Must have MVP
* Python
* Process data in streaming fashion

Should have (production ready)
* Horizontally scalable in copute
* Show real-time results (latency max 5 seconds)
* Appropriate unittests

Could have
* Possible to implement the solution in the cloud
* Solution should be horizontally scalable in terms of data storage capacity. Assume not all data can fit in one computer’s memory and/or disk. Hint: think of how you are partitioning the data (if that’s applicable to your use case).


### Setting up
`$env:CR_PAT = "<YOUR_PERSONAL_ACCESS_TOKEN>"`
`$env:CR_PAT | docker login ghcr.io -u FerdivdKamp --password-stdin`

Pull the image, make sure Docker Desktop is running before performing the next step
`docker pull ghcr.io/bigdatarepublic/bdr-task-api:v-0.0.3`

`docker run -p 8000:8000 --rm -it ghcr.io/bigdatarepublic/bdr-task-api:v-0.0.3`


After it runs you can visit and chec its Swagger UI
`http://localhost:8000/`

