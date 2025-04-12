import grpc
import hazelcast
import random
import time
import uuid
from concurrent import futures
from grpc_reflection.v1alpha import reflection
import logging
import logging_pb2
import logging_pb2_grpc
import facade_pb2
import facade_pb2_grpc
import messages_pb2
import messages_pb2_grpc
import requests
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_ports_from_config(config_url):
    """Fetches the logging and message service ports from the config server."""
    try:
        response = requests.get(config_url)
        response.raise_for_status()
        config = response.json()
        logging_ports = config.get("logging-services", [])
        message_port = config.get("message-service", [])
        if not logging_ports or not message_port:
            logger.error("Logging services or message service ports are not properly defined in the config.")
            return None, None
        return logging_ports, message_port[0]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching config: {e}")
        return None, None

def get_messages_stub():
    config_url = f'http://localhost:{sys.argv[1]}/get_ports'
    logging_ports, message_port = get_ports_from_config(config_url)
    if not message_port:
        context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details("No available messages services.")
        return None
            
    try:
        channel = grpc.insecure_channel(f'localhost:{message_port}')
        grpc.channel_ready_future(channel).result(timeout=1)
        stub = messages_pb2_grpc.MessageServiceStub(channel)
        return stub
    except Exception as e:
        logger.error(f"Failed to connect to messages service on port {message_port}: {e}")
        return None

def get_logging_stub():
    config_url = f'http://localhost:{sys.argv[1]}/get_ports'
    logging_ports, message_port = get_ports_from_config(config_url)
    if not logging_ports:
        context.set_code(grpc.StatusCode.UNAVAILABLE)
        context.set_details("No available logging services.")
        return None
            
    random.shuffle(logging_ports)
    for port in logging_ports:
        try:
            channel = grpc.insecure_channel(f'localhost:{port}')
            grpc.channel_ready_future(channel).result(timeout=1)
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            return stub
        except Exception as e:
            logger.error(f"Failed to connect to logging service on port {port}: {e}")
    return None

class FacadeService(facade_pb2_grpc.FacadeServiceServicer):
    def WriteMessage(self, request, context):
        message_uuid = str(uuid.uuid4())
        stub = get_logging_stub()
        if not stub:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("No available logging services.")
            return facade_pb2.LogResponse(success=False, message="No logging service available")
        
        try:
            log_request = logging_pb2.LogRequest(uuid=message_uuid, message=request.message)
            log_response = stub.LogMessage(log_request)
            return log_response
        except grpc.RpcError as e:
            logger.error(f"Logging service error: {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Logging service unavailable.")
            return facade_pb2.LogResponse(success=False, message="Failed to log message")

    def ShowMessages(self, request, context):
        logging_stub = get_logging_stub()
        if not logging_stub:
            logger.error("No available logging services.")

        messages_stub = get_messages_stub()
        if not messages_stub:
            logger.error("No available messages services.")

        try:
            log_response = []
            if logging_stub:
                log_response = logging_stub.GetMessages(logging_pb2.GetRequest()).messages
            
            message_response = "" 
            if messages_stub:
                message_response = messages_stub.GetStaticMessage(messages_pb2.EmptyRequest()).message

            formatted_response = " ".join([f"{{ {log_msg} }}" for log_msg in log_response]) + f" : {{ {message_response} }}"

            return logging_pb2.GetResponse(messages=[formatted_response])

        except grpc.RpcError as e:
            logger.error(f"Error fetching messages: {e}")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Failed to fetch messages.")
            return facade_pb2.GetResponse(messages=[])

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    facade_pb2_grpc.add_FacadeServiceServicer_to_server(FacadeService(), server)
    
    SERVICE_NAMES = (
        facade_pb2.DESCRIPTOR.services_by_name['FacadeService'].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)
    
    server.add_insecure_port('[::]:8000')
    server.start()
    logger.info("Facade Service is alive on port 8000")
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python facade.py <config_port>")
        sys.exit(1)
    serve()
