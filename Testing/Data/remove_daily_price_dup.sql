DELETE FROM daily_price
WHERE id NOT IN (
    SELECT MIN(id)
    FROM daily_price
    GROUP BY symbol_id, price_date
);
