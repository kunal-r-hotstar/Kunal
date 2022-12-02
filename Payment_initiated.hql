with payment_final as
(SELECT to_date(FROM_UTC_TIMESTAMP(from_unixtime(cast((charges.created_on/1000) AS bigint)), 'IST')) as cd,
(FROM_UTC_TIMESTAMP(from_unixtime(cast((charges.created_on/1000) AS bigint)), 'IST')) as ts,
charges.created_on as created_on,
charges.id,
subs.hid,
charges.status
from in_hspay_payments.charges_s3 charges
left join in_hspay_payments.payu_charges_s3 payu_charges on charges.id = payu_charges.ref_charge_id
left join in_hspay_subscriptions.orders_s3 orders on orders.id = charges.order_id
left join in_hspay_subscriptions.subscriptions_s3 subs on orders.subscription_id = subs.id
left join in_hspay_payments.paytm_charges_s3 paytm_charges on charges.id = paytm_charges.ref_charge_id
left join in_hspay_payments.phonepe_charges_s3 phonepe_charges on charges.id = phonepe_charges.ref_charge_id
left join in_hspay_payments.razorpay_charges_s3 razor_charges on charges.id = razor_charges.ref_charge_id
left join in_hspay_payments.billdesk_charges_s3 billdesk on charges.id = billdesk.ref_charge_id
where
(coalesce(payu_charges.issitransaction,0)=0 and coalesce(paytm_charges.is_si_transaction,0)=0
and coalesce(razor_charges.issitransaction,0)=0 and coalesce(phonepe_charges.issitransaction,0)=0
and coalesce(billdesk.issitransaction,0)=0)
and to_date(FROM_UTC_TIMESTAMP(from_unixtime(cast((charges.created_on/1000) AS bigint)), 'IST')) =  date'$runDate' - interval  '1'  day 
and ((charges.is_deleted) = FALSE OR (charges.is_deleted) IS NULL)
and ((subs.is_deleted) = FALSE OR (subs.is_deleted) IS NULL)
and ((orders.is_deleted) = FALSE OR (orders.is_deleted) IS NULL)),

users as (select hid, sha2(pid,256) as pid from in_ums.umusers_s3 where hid is not null),

payment_init_users as 
(select cd, ts, status, pid from payment_final inner join
users on payment_final.hid = users.hid)

insert overwrite table premium.payment_initiated_avod_to_svod_DG
partition(cd)
select 
a.pid, 
a.status,a.cd
from payment_init_users a
 
