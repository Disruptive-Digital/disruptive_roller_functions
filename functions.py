import os
import json
import requests
import pandas as pd
from google.cloud import bigquery

# For local dev
from dotenv import load_dotenv
load_dotenv()


def get_roller_auth_token(location: str) -> str:

    url = "https://api.roller.app/token"

    payload = json.dumps({
        "client_id": os.getenv(f"{location}-clientid"),
        "client_secret": os.getenv(f"{location}-secretid")
    })
    headers = {
        'Content-Type': 'application/json'  
    }

    response = requests.post(url, headers=headers, data=payload)

    return response.json().get("access_token")


def get_roller_revenue(startDate: str, 
                       endDate: str, 
                       location: str,
                       pageNumber: int = 0
                    ) -> str:

    if pageNumber > 0:
        url = f"https://api.roller.app/reporting/revenue-entries?pageNumber={pageNumber}&endDate={endDate}&startDate={startDate}"
    else:
        url = f"https://api.roller.app/reporting/revenue-entries?endDate={endDate}&startDate={startDate}"

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {get_roller_auth_token(location=location)}'
    }

    response = requests.get(url, headers=headers)
    return response

def merge_to_bigquery(df: pd.DataFrame,
                      project_name: str,
                      dataset_name: str,
                      table_name: str) -> None:

    client = bigquery.Client.from_service_account_info(json.loads(os.getenv("service_account_json")))
    target_table = f"{project_name}.{dataset_name}.{table_name}"
    staging_table = f"{project_name}.{dataset_name}.staging_{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()

    merge_query = f"""
    MERGE `{target_table}` T
    USING (
           SELECT *
            FROM (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY
                    items.bookingPaymentId,
                    items.bookingItemId,
                    items.recordDate,
                    CAST(items.accountsReceivable AS STRING),
                    CAST(items.deferredRevenue AS STRING)
                  ORDER BY
                    items.recordDate DESC 
                ) AS rn
              FROM `{staging_table}`
            )
            WHERE rn = 1
        ) S
        ON T.bookingPaymentId = S.items.bookingPaymentId
          AND T.bookingItemId = S.items.bookingItemId
          AND T.recordDate = CAST(S.items.recordDate AS TIMESTAMP)
          AND T.accountsReceivable = S.items.accountsReceivable
          AND T.deferredRevenue = S.items.deferredRevenue
        
        WHEN MATCHED THEN
        UPDATE SET
            bookingPaymentId = S.items.bookingPaymentId,
            bookingReference = S.items.bookingReference,
            packageBookingItemId = S.items.packageBookingItemId,
            customerId = S.items.customerId,
            productId = S.items.productId,
            productType = S.items.productType,
            transactionDate = CAST(S.items.transactionDate AS TIMESTAMP),
            recordDate = CAST(S.items.recordDate AS TIMESTAMP),
            entryType = S.items.entryType,
            transactionLocation = S.items.transactionLocation,
            bookingLocation = S.items.bookingLocation,
            paymentType = S.items.paymentType,
            externalPaymentReference = S.items.externalPaymentReference,
            unitCost = S.items.unitCost,
            transactionValue = S.items.transactionValue,
            taxPercent = S.items.taxPercent,
            feeTaxPercent = S.items.feeTaxPercent,
            fundsReceived = S.items.fundsReceived,
            taxOnFundsReceived = S.items.taxOnFundsReceived,
            voucherFundsReceived = S.items.voucherFundsReceived,
            discount = S.items.discount,
            feeRevenue = S.items.feeRevenue,
            taxOnFees = S.items.taxOnFees,
            deferredRevenue = S.items.deferredRevenue,
            deferredRevenueGiftCards = S.items.deferredRevenueGiftCards,
            manualGiftCardAdjustment = S.items.manualGiftCardAdjustment,
            deferredRevenueOther = S.items.deferredRevenueOther,
            accountsReceivable = S.items.accountsReceivable,
            netRevenue = S.items.netRevenue,
            taxPayable = S.items.taxPayable,
            recognisedDiscount = S.items.recognisedDiscount,
            ticketQuantity = S.items.ticketQuantity,
            redeemedQuantity = S.items.redeemedQuantity,
            expiredQuantity = S.items.expiredQuantity
        
        WHEN NOT MATCHED THEN
        INSERT (
            bookingPaymentId, bookingReference, packageBookingItemId, customerId, productId, productType, bookingItemId,
            transactionDate, recordDate, entryType, transactionLocation, bookingLocation, paymentType, externalPaymentReference,
            unitCost, transactionValue, taxPercent, feeTaxPercent, fundsReceived, taxOnFundsReceived, voucherFundsReceived,
            discount, feeRevenue, taxOnFees, deferredRevenue, deferredRevenueGiftCards, manualGiftCardAdjustment,
            deferredRevenueOther, accountsReceivable, netRevenue, taxPayable, recognisedDiscount, ticketQuantity,
            redeemedQuantity, expiredQuantity
        )
        VALUES (
            S.items.bookingPaymentId, S.items.bookingReference, S.items.packageBookingItemId, S.items.customerId, S.items.productId, S.items.productType, S.items.bookingItemId,
            CAST(S.items.transactionDate AS TIMESTAMP), CAST(S.items.recordDate AS TIMESTAMP), S.items.entryType, S.items.transactionLocation, S.items.bookingLocation, S.items.paymentType, S.items.externalPaymentReference,
            S.items.unitCost, S.items.transactionValue, S.items.taxPercent, S.items.feeTaxPercent, S.items.fundsReceived, S.items.taxOnFundsReceived, S.items.voucherFundsReceived,
            S.items.discount, S.items.feeRevenue, S.items.taxOnFees, S.items.deferredRevenue, S.items.deferredRevenueGiftCards, S.items.manualGiftCardAdjustment,
            S.items.deferredRevenueOther, S.items.accountsReceivable, S.items.netRevenue, S.items.taxPayable, S.items.recognisedDiscount, S.items.ticketQuantity,
            S.items.redeemedQuantity, S.items.expiredQuantity
    
        );

    """
    client.query(merge_query).result()
    print("Merge completed successfully.")


# if __name__ == '__main__':
#     print(run_pipeline())
