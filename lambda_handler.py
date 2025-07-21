"""
AWS Lambda handler for Identity Reconciliation System
This module adapts the FastAPI application to work with AWS Lambda + API Gateway
"""

import os
import logging
import json
from mangum import Mangum
from main import app

# Configure logging for Lambda
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure Mangum adapter for Lambda with proper settings
handler = Mangum(
    app,
    lifespan="off",  # Disable lifespan events for Lambda
    api_gateway_base_path=None,  # Use root path
    text_mime_types=[
        "application/json",
        "application/javascript",
        "application/xml",
        "application/vnd.api+json",
        "text/plain",
        "text/html"
    ],
    exclude_headers=["x-amzn-trace-id"]  # Exclude AWS-specific headers
)

def lambda_handler(event, context):
    """
    AWS Lambda entry point
    
    Args:
        event: API Gateway event data
        context: Lambda runtime context
        
    Returns:
        API Gateway response format
    """
    # Log environment info for debugging
    logger.info(f"Lambda function: {context.function_name}")
    logger.info(f"Lambda version: {context.function_version}")
    logger.info(f"Remaining time: {context.get_remaining_time_in_millis()}ms")
    
    # Log key environment variables (without sensitive data)
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'not-set')}")
    logger.info(f"RDS Hostname: {os.getenv('RDS_HOSTNAME', 'not-set')}")
    logger.info(f"Database URL configured: {'DATABASE_URL' in os.environ}")
    
    # Log the event structure for debugging
    logger.info(f"Event keys: {list(event.keys())}")
    
    # Detect event format and log appropriately
    if 'version' in event and event['version'] == '2.0':
        # API Gateway v2 format
        method = event.get('requestContext', {}).get('http', {}).get('method', 'UNKNOWN')
        path = event.get('requestContext', {}).get('http', {}).get('path', 'UNKNOWN')
        logger.info(f"API Gateway v2 event: {method} {path}")
    elif 'httpMethod' in event:
        # API Gateway v1 format
        method = event.get('httpMethod', 'UNKNOWN')
        path = event.get('path', 'UNKNOWN')
        logger.info(f"API Gateway v1 event: {method} {path}")
    else:
        logger.info(f"Unknown event format. Event: {json.dumps(event, default=str)}")
    
    try:
        # Use Mangum to handle the request
        logger.info("Calling Mangum handler...")
        response = handler(event, context)
        logger.info(f"Mangum response status: {response.get('statusCode', 'UNKNOWN')}")
        logger.info(f"Response headers: {response.get('headers', {})}")
        return response
        
    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}", exc_info=True)
        logger.error(f"Event that caused error: {json.dumps(event, default=str)}")
        
        # Return error response in API Gateway format
        error_response = {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization"
            },
            "body": json.dumps({
                "error": "Internal server error", 
                "message": "An unexpected error occurred",
                "requestId": context.aws_request_id
            })
        }
        
        logger.info(f"Returning error response: {error_response}")
        return error_response

# For local testing
if __name__ == "__main__":
    # Test event for local development (API Gateway v2 format)
    test_event = {
        "version": "2.0",
        "routeKey": "POST /identify",
        "rawPath": "/identify",
        "rawQueryString": "",
        "headers": {
            "accept": "application/json",
            "content-type": "application/json",
            "content-length": "67",
            "host": "api.example.com",
            "user-agent": "test-client/1.0",
            "x-forwarded-for": "203.0.113.12",
            "x-forwarded-port": "443",
            "x-forwarded-proto": "https"
        },
        "requestContext": {
            "accountId": "123456789012",
            "apiId": "abcdef123",
            "domainName": "api.example.com",
            "domainPrefix": "api",
            "http": {
                "method": "POST",
                "path": "/identify",
                "protocol": "HTTP/1.1",
                "sourceIp": "203.0.113.12",
                "userAgent": "test-client/1.0"
            },
            "requestId": "test-request-123",
            "routeKey": "POST /identify",
            "stage": "$default",
            "time": "01/Jan/2025:00:00:00 +0000",
            "timeEpoch": 1735689600000
        },
        "body": '{"email": "test@lambda.com", "phoneNumber": "+1-555-LAMBDA"}',
        "isBase64Encoded": False
    }
    
    test_context = type('Context', (), {
        'function_name': 'test-function',
        'function_version': '1',
        'invoked_function_arn': 'arn:aws:lambda:us-east-1:123456789012:function:test-function',
        'memory_limit_in_mb': '512',
        'remaining_time_in_millis': lambda: 30000,
        'log_group_name': '/aws/lambda/test-function',
        'log_stream_name': '2023/01/01/[$LATEST]test123',
        'aws_request_id': 'test-request-id'
    })()
    
    print("Testing Lambda handler locally...")
    response = lambda_handler(test_event, test_context)
    print(f"Response: {response}")