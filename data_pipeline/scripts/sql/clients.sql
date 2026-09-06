merge
  `sasisaidivi.refine_synthetic.clients` as tar
using 
  (
    --selecting which was updated cols
    select 
      s.*,
      'update' as type_op
    from  
      `sasisaidivi.raw_synthetic.clients_transfer` s
    join 
      `sasisaidivi.refine_synthetic.clients` t
    on 
      s.id = t.id and t.is_active = true
    where 
      (s.phone_number IS DISTINCT FROM t.phone_number) or 
      (s.email IS DISTINCT FROM t.email) or 
      (s.address IS DISTINCT FROM t.address) or 
      (s.workplace IS DISTINCT FROM t.workplace) or 
      ( s.income IS DISTINCT FROM t.income ) or 
      ( s.expenses IS DISTINCT FROM t.expenses ) or  
      ( s.credit IS DISTINCT FROM t.credit ) or 
      ( s.deposit IS DISTINCT FROM t.deposit ) 
    
    union all
    
    --selecting which updated and that need to insert as of scd type2
    select 
      s1.*,
      'insert' as type_op
    from  
      `sasisaidivi.raw_synthetic.clients_transfer` s1
    join 
      `sasisaidivi.refine_synthetic.clients` t1
    on 
      s1.id = t1.id and t1.is_active = true
    where 
      (s1.phone_number IS DISTINCT FROM t1.phone_number) or 
      (s1.email IS DISTINCT FROM t1.email) or 
      (s1.address IS DISTINCT FROM t1.address) or 
      (s1.workplace IS DISTINCT FROM t1.workplace) or  
      ( s1.income IS DISTINCT FROM t1.income ) or 
      ( s1.expenses IS DISTINCT FROM t1.expenses ) or  
      ( s1.credit IS DISTINCT FROM t1.credit ) or 
      ( s1.deposit IS DISTINCT FROM t1.deposit ) 

    
    union all

    --selecting totaly new cols
    select 
      s2.*,
      'insert' as type_op
    from  
      `sasisaidivi.raw_synthetic.clients_transfer` s2
    left join 
      `sasisaidivi.refine_synthetic.clients` t2
    on 
      s2.id = t2.id
    where 
      t2.id is null
  
  ) as sou 
on 
  tar.id = sou.id and sou.type_op = 'update' and tar.is_active = true

when matched then 
  update 
  set
    tar.is_active = false,
    tar.active_timestamp = current_timestamp()
when not matched then 
  insert
  (
  id,
  fullname,
  address,
  phone_number,
  email,
  workplace,
  birthdate,
  registration_date,
  gender,
  income,
  expenses,
  credit,
  deposit,
  is_active,
  active_timestamp
  )
  values 
  ( 
    sou.id,
    sou.fullname,
    sou.address,
    sou.phone_number,
    sou.email,
    sou.workplace,
    sou.birthdate,
    sou.registration_date,
    sou.gender,
    sou.income,
    sou.expenses,
    sou.credit,
    sou.deposit,
    true,
    current_timestamp()
  )

