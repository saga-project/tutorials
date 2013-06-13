import sys
import os
import time
import logging
import uuid
#logging.basicConfig(level=logging.DEBUG)

#sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))
from pilot import PilotComputeService, PilotDataService, ComputeDataService, State
from bigjob import logger 

COORDINATION_URL = "redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"

if __name__ == "__main__":      
    
    print COORDINATION_URL
    
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    
    pilot_compute_description = {
                             "service_url": 'nova+ssh://10.103.114.3:8773/services/Cloud',
                             #"service_url": 'ssh://ubuntu@172.16.1.28',
                             "working_directory":"/home/ubuntu",
                             "number_of_processes": 1,                             
                             "vm_id": "ami-00000042",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"OSDC",
                             "vm_ssh_keyfile":"/glusterfs/users/aluckow/.ssh/osdc_rsa.pub",
                             "vm_type":"m1.tiny",
                             "access_key_id":"8002fb8a8572432c92d2e080ab1f326a",
                             "secret_access_key":"db32d545bd8e44b3b22514622b9621c5"                                                       }
    pilotjob = pilot_compute_service.create_pilot(pilot_compute_description)
    
    compute_data_service = ComputeDataService()
    compute_data_service.add_pilot_compute_service(pilot_compute_service)
    
    
    # create compute unit
    compute_unit_description = {
            "executable": "/bin/date",
            "arguments": [""],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",   
    }   
    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    logger.info("Finished setup of ComputeDataService. Waiting for scheduling of CU")
    compute_data_service.wait()
    
    
    logger.info("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_compute_service.cancel()
