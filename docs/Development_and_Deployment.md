# Application Development and Deployment

This is intended to be a living document to describe the development environment and CI/CD pipeline used to build and deploy the envDS/MSP applications. 

## Overview

The system is designed to be modular, using containerized services running in a kubernetes environment. One set of services will primarily interface with hardware (sensors, pumps, etc) as the main raw data acquisition and device control layer. The other set of services will handle the system level needs such as providing a message bus, data store, data processing and user interface. The services will run in some version of kubernetes: on field computers we use k3d (containerized k3s) and on larger clusters we use k3s (on-prem) or a managed variant (e.g., EKS) as provided by a commercial cloud vendor.   