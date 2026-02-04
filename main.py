import json
import logging
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = bigquery.Client()

QUERY = """
INSERT INTO `dna-staging-test.rentoza_drive.flexclub_payment_reference_upcoming`
(
  creation_date,
  next_payment_date,
  order_number,
  paystack_customer_id,
  paystack_customer_code,
  auth_code,
  email,
  customer,
  amount,
  product,
  instance,
  subscription_payment_key,
  uuid,
  current_status,
  product_variant,
  payment_category,
  gateway,
  created_time,
  agent
)
SELECT
  creation_date,
  CAST(next_payment_date AS DATE),
  CAST(order_number AS STRING),
  paystack_customer_id,
  paystack_customer_code,
  CAST(auth_code AS STRING),
  CAST(email AS STRING),
  customer,
  amount,
  CAST(product AS STRING),
  instance,
  CONCAT(key, ' - ', CAST(next_payment_date AS STRING)) AS subscription_payment_key,
  GENERATE_UUID(),
  current_status,
  product_variant,
  CASE
    WHEN current_status = 'Suspended' THEN 'Terminated'
    WHEN current_status = 'Active' THEN 'subscription_payment'
    ELSE NULL
  END AS payment_category,
  'card_charge' AS gateway,
  CURRENT_DATETIME('Africa/Johannesburg') AS created_time,
  NULL AS agent
FROM `dna-staging-test.rentoza_drive.upcoming_payments`
WHERE current_status IN ('Active', 'Suspended', 'Terminated')
  AND next_payment_date = CURRENT_DATE()
"""

def create_reference(request):
    try:
        logger.info("Running payment reference function")

        job = client.query(QUERY)
        job.result()

        logger.info(f"Rows inserted: {job.num_dml_affected_rows}")

        return (
            json.dumps({
                "status": "success",
                "rows_inserted": job.num_dml_affected_rows
            }),
            200,
            {"Content-Type": "application/json"}
        )

    except Exception as e:
        logger.exception("Stacktrace for load references")
        return (
            json.dumps({"status": "error", "message": str(e)}),
            500,
            {"Content-Type": "application/json"}
        )
