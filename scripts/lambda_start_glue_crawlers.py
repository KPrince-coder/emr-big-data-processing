"""
AWS Lambda Function to Start Glue Crawlers

This Lambda function is used by the Step Functions workflow to start Glue crawlers.
"""

import boto3
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function handler to start Glue crawlers.
    
    Args:
        event (dict): Event data from Step Functions
        context (object): Lambda context
        
    Returns:
        dict: Result of the operation
    """
    logger.info("Starting Glue crawlers")
    
    # Get parameters from the event
    database = event.get('database')
    crawlers = event.get('crawlers', [])
    
    if not database or not crawlers:
        error_message = "Missing required parameters: database and/or crawlers"
        logger.error(error_message)
        return {
            'statusCode': 400,
            'error': error_message
        }
    
    glue_client = boto3.client('glue')
    results = {}
    
    # Start each crawler
    for crawler_name in crawlers:
        try:
            # Check if crawler is already running
            response = glue_client.get_crawler(Name=crawler_name)
            crawler_state = response['Crawler']['State']
            
            if crawler_state == 'RUNNING':
                logger.info(f"Crawler '{crawler_name}' is already running")
                results[crawler_name] = 'ALREADY_RUNNING'
                continue
            
            # Start the crawler
            glue_client.start_crawler(Name=crawler_name)
            logger.info(f"Started crawler '{crawler_name}'")
            results[crawler_name] = 'STARTED'
            
        except glue_client.exceptions.EntityNotFoundException:
            error_message = f"Crawler '{crawler_name}' not found"
            logger.error(error_message)
            results[crawler_name] = 'NOT_FOUND'
            
        except Exception as e:
            error_message = f"Error starting crawler '{crawler_name}': {str(e)}"
            logger.error(error_message)
            results[crawler_name] = 'ERROR'
    
    # Wait for all crawlers to complete (with timeout)
    timeout_seconds = 900  # 15 minutes
    start_time = time.time()
    all_completed = False
    
    while not all_completed and time.time() - start_time < timeout_seconds:
        all_completed = True
        
        for crawler_name in crawlers:
            try:
                response = glue_client.get_crawler(Name=crawler_name)
                crawler_state = response['Crawler']['State']
                
                if crawler_state == 'RUNNING':
                    logger.info(f"Crawler '{crawler_name}' is still running")
                    all_completed = False
                    break
                    
            except Exception as e:
                logger.error(f"Error checking crawler '{crawler_name}' status: {str(e)}")
        
        if not all_completed:
            time.sleep(30)  # Wait for 30 seconds before checking again
    
    # Check final status of all crawlers
    for crawler_name in crawlers:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            last_crawl = response['Crawler'].get('LastCrawl', {})
            status = last_crawl.get('Status')
            
            results[crawler_name] = f"COMPLETED: {status}"
            logger.info(f"Crawler '{crawler_name}' completed with status: {status}")
            
        except Exception as e:
            logger.error(f"Error checking final status of crawler '{crawler_name}': {str(e)}")
    
    return {
        'statusCode': 200,
        'database': database,
        'crawlers': results
    }
