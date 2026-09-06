merge
  `sasisaidivi.refine_synthetic.transactions` as tar
using 
  `sasisaidivi.raw_synthetic.transactions_transfer` as sou 
on 
  tar.id = sou.id
when not matched then 
  insert
  (
    id,
    client_id,
    product_category,
    product_company,
    subtype,
    amount,
    date,
    transaction_type
  )
  values 
  ( 
    sou.id,
    sou.client_id,
    sou.product_category,
    sou.product_company,
    sou.subtype,
    sou.amount,
    sou.date,
    sou.transaction_type
  )

