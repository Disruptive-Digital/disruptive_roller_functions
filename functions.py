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
    USING `{staging_table}` S
    ON T.bookingPaymentId = S.bookingPaymentId AND T.bookingItemId = S.bookingItemId

    WHEN MATCHED THEN
    UPDATE SET
        bookingPaymentId = S.bookingPaymentId,
        bookingReference = S.bookingReference,
        packageBookingItemId = S.packageBookingItemId,
        customerId = S.customerId,
        productId = S.productId,
        productType = S.productType,
        transactionDate = S.transactionDate,
        recordDate = S.recordDate,
        entryType = S.entryType,
        transactionLocation = S.transactionLocation,
        bookingLocation = S.bookingLocation,
        paymentType = S.paymentType,
        externalPaymentReference = S.externalPaymentReference,
        unitCost = S.unitCost,
        transactionValue = S.transactionValue,
        taxPercent = S.taxPercent,
        feeTaxPercent = S.feeTaxPercent,
        fundsReceived = S.fundsReceived,
        taxOnFundsReceived = S.taxOnFundsReceived,
        voucherFundsReceived = S.voucherFundsReceived,
        discount = S.discount,
        feeRevenue = S.feeRevenue,
        taxOnFees = S.taxOnFees,
        deferredRevenue = S.deferredRevenue,
        deferredRevenueGiftCards = S.deferredRevenueGiftCards,
        manualGiftCardAdjustment = S.manualGiftCardAdjustment,
        deferredRevenueOther = S.deferredRevenueOther,
        accountsReceivable = S.accountsReceivable,
        netRevenue = S.netRevenue,
        taxPayable = S.taxPayable,
        recognisedDiscount = S.recognisedDiscount,
        ticketQuantity = S.ticketQuantity,
        redeemedQuantity = S.redeemedQuantity,
        expiredQuantity = S.expiredQuantity

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
        S.bookingPaymentId, S.bookingReference, S.packageBookingItemId, S.customerId, S.productId, S.productType, S.bookingItemId,
        S.transactionDate, S.recordDate, S.entryType, S.transactionLocation, S.bookingLocation, S.paymentType, S.externalPaymentReference,
        S.unitCost, S.transactionValue, S.taxPercent, S.feeTaxPercent, S.fundsReceived, S.taxOnFundsReceived, S.voucherFundsReceived,
        S.discount, S.feeRevenue, S.taxOnFees, S.deferredRevenue, S.deferredRevenueGiftCards, S.manualGiftCardAdjustment,
        S.deferredRevenueOther, S.accountsReceivable, S.netRevenue, S.taxPayable, S.recognisedDiscount, S.ticketQuantity,
        S.redeemedQuantity, S.expiredQuantity
    );

    """
    client.query(merge_query).result()
    print("Merge completed successfully.")


# if __name__ == '__main__':
#     print(run_pipeline())