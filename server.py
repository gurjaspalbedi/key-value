# -*- coding: utf-8 -*-
#https://www.semantics3.com/blog/a-simplified-guide-to-grpc-in-python-6c4e25f0c506/
#This was the first grpc code that I wrote with help from the above given link.

# For loggin used the following blog post
#import logging # ref: https://realpython.com/python-logging/


import grpc
from concurrent import futures
import time
import store_pb2
import store_pb2_grpc
import store
from configuration import data_store_address
from dependency_manager import Dependencies
stub = None

log = Dependencies.log()
class KeyValueService(store_pb2_grpc.GetSetServicer):

    def operation(self, request, context):
        log.write(f'Data Store Operation: {request.operation}', 'debug')
        response = store_pb2.Response()
        response.data = store.operation(request.operation , request.stage)
        return response
 

def init_data_store(cluster_id = 0):
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    store_pb2_grpc.add_GetSetServicer_to_server(
            KeyValueService(), server)
    
    
    failed = server.add_insecure_port(f'[::]:{data_store_address["port"]}')
    if failed != 0:
        server.start()
        log.write(f'Started Data Store. Listening on port {data_store_address["port"]}.', 'debug')
    else:
        log.write(f'Failed to start the data store', 'critical')

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    init_data_store()
