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
    # create pilot data service (factory for data pilots (physical, distributed storage))
    # and pilot data
    pilot_data_service = PilotDataService(coordination_url=COORDINATION_URL)
    
    ###################################################################################################
    # Pick one of the Pilot Data Descriptions below    

    pilot_data_directory="/glusterfs/users/aluckow/pilot-data/"

    try:
        os.mkdir(pilot_data_directory)
    except:
        pass
    
    pilot_data_description_aws={
                                "service_url": "ssh://aluckow@sullivan.opensciencedatacloud.org/"+pilot_data_directory,
                                "size": 100,   
                                "userkey": "/glusterfs/users/aluckow/.ssh/osdc_rsa"
                              }
    
    
    pd = pilot_data_service.create_pilot(pilot_data_description=pilot_data_description_aws)
     
     
    # Create Data Unit Description
    data_unit_description = {
                              "file_urls": [os.path.join(os.getcwd(), "test.txt")],
                             }    
      
    # submit pilot data to a pilot store 
    input_data_unit = pd.submit_data_unit(data_unit_description)
    input_data_unit.wait()
    
    #logger.info("Data Unit URL: " + input_data_unit.get_url())
    pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)
    
    pilot_compute_description = {
                             #"service_url": 'nova+ssh://10.103.114.3:8773/services/Cloud',
                             "service_url": 'ssh://ubuntu@172.16.1.28',
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
    compute_data_service.add_pilot_data_service(pilot_data_service)
    
    # create empty data unit for output data
    output_data_unit_description = {
         "file_urls": []              
    }
    output_data_unit = pd.submit_data_unit(output_data_unit_description)
    output_data_unit.wait()
    
    # create compute unit
    compute_unit_description = {
            "executable": "/bin/cat",
            "arguments": ["test.txt"],
            "number_of_processes": 1,
            "output": "stdout.txt",
            "error": "stderr.txt",   
            "input_data": [input_data_unit.get_url()],
            # Put files stdout.txt and stderr.txt into output data unit
            #"output_data": [
            #                {
            #                 output_data_unit.get_url(): 
            #                 ["std*"]
            #                }
            #               ]    
    }   
    
    compute_unit = compute_data_service.submit_compute_unit(compute_unit_description)
    logger.info("Finished setup of ComputeDataService. Waiting for scheduling of PD")
    compute_data_service.wait()
    
    logger.debug("Output Data Unit: " + str(output_data_unit.list()))
    
    logger.info("Terminate Pilot Compute/Data Service")
    compute_data_service.cancel()
    pilot_data_service.cancel()
    pilot_compute_service.cancel()
