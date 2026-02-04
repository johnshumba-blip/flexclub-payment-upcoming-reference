
import logging
from google.cloud import bigquery
import config
import pandas as pd


# Set the logging format
log_format = logging.Formatter(
    '{ "severity": "%(levelname)s", "timestamp": "%(asctime)s", "message": "%(message)s" }'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the logging level
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)



client = bigquery.Client(credentials=config.credentials_bigquery, project=config.credentials_bigquery.project_id)




def create_reference():
    logger.info(f"Running payment reference function ...") 
    
    
    query = """
        
        select
            creation_date,
            cast(next_payment_date as date) as next_payment_date,
            cast(order_number as string) as order_number,
            paystack_customer_id,
            paystack_customer_code,
            cast(auth_code as string) as auth_code,

            cast(email as string)as email,
            customer,
            -- cast(amount as string) as Amount,
            -- cast(amount as int64)*100 as amount,
            amount,
            cast(product as string) as product,
            instance,
            concat(key, " - ",next_payment_date) as subscription_payment_key,
            GENERATE_UUID() AS uuid,
            current_status,
            product_variant,
            case 
              when current_status = "buyout installment" then "buyout_installment" 
              when current_status in ("Active","Upgrade") then "subscription_payment"
                else null
            end as payment_category,
            "card_charge" as gateway,
            current_datetime("Africa/Johannesburg") as created_time,
            "null" as agent




        from `dna-staging-test.RPG.Upcoming_Payments`
        where current_status in ('Active','buyout installment','Upgrade')
        and next_payment_date = current_date()
        
        
    """
    try:
        query_job = client.query(query)
        payment_reference_df = query_job.to_dataframe()
        
        
        table_id = 'dna-staging-test.rentoza_drive.flexclub_payment_reference_upcoming'    
        job_config = bigquery.LoadJobConfig( schema=[
        bigquery.SchemaField("uuid", bigquery.enums.SqlTypeNames.STRING),], write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(payment_reference_df, table_id, job_config=job_config)
        print(job)
        print("successfully uploaded")
        
        return job
        

    except Exception as e:
        logger.exception('Stacktrace for load references')
        return           
              
              
def main(event, context):
    try:
        upcoming_result = create_reference()
        logger.info(f"Reference Creation Run Result: {upcoming_result}")

    except Exception as e:
        logger.exception('Stacktrace for reference')
        logger.error(f" error creating payment references {str(e)}")