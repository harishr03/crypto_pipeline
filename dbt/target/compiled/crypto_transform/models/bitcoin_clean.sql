

SELECT 
    raw_data:bitcoin.usd::FLOAT as bitcoin_price, 
    raw_data:bitcoin.last_updated_at::NUMBER as last_updated_epoch, 
    raw_data:scraped_at::TIMESTAMP as ingestion_time 
FROM CRYPTO_DB.RAW.BITCOIN_PRICES
ORDER BY ingestion_time DESC