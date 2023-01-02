-- Представления в БД управленческого учета

create view payment_requests_packs_expanded as
select prp.*, coalesce(sum, 0) as sum
from payment_requests_packs prp
left join (
select sum(sum) as sum, payment_requests_packs_id
from payment_requests group by payment_requests_packs_id
) pr on pr.payment_requests_packs_id = prp.id;

select * from payment_requests_packs_expanded;

create view payment_requests_expanded as
select pr.*, prp.number as payment_requests_packs_number,
cr.id as contractors_id,
concat(it.code, ' ', it.name) as items_name,
concat(bi.code, ' ', bi.name) as banks_items_name,
cr.name as contractors_name,
concat('Договор № ', c.number, ' от ', DATE_FORMAT(c.date, '%d.%m.%Y'), coalesce(concat(', сч. № ', pr.bill_number, ' от ', pr.bill_date), ''), coalesce(concat(', ', pr.attached_docs), ''), coalesce(concat(', ', pra.acts_names), '')) as all_docs,
concat('Договор № ', c.number, ' от ', DATE_FORMAT(c.date, '%d.%m.%Y')) as contracts_name,
pra.acts_ids, pra.acts_names, pra.acts_sum
from payment_requests pr
left join (
select pra.payment_requests_id,
GROUP_CONCAT(pra.acts_id ORDER BY pra.acts_id SEPARATOR ', ') as acts_ids,
GROUP_CONCAT(concat(dt.name, ' № ', acts.number, ' от ', DATE_FORMAT(acts.date, '%d.%m.%Y')) ORDER BY pra.acts_id SEPARATOR ', ') as acts_names,
SUM(acts.sum) as acts_sum
from r_payment_requests_acts pra
left join acts on pra.acts_id = acts.id
left join document_types dt on dt.id = acts.document_types_id
group by pra.payment_requests_id
) pra on pra.payment_requests_id = pr.id
left join contracts c on c.id = pr.contracts_id
left join contractors cr on cr.id = c.contractors_id
left join items it
on c.items_id = it.id
left join banks_items bi
on bi.id = it.banks_items_id
left join payment_requests_packs prp
on prp.id = pr.payment_requests_packs_id
;

select * from payment_requests_expanded;

create view items_expanded as
select
items.id, concat(items.code, ' ', items.name) as name, items.order_num,
items.is_active, items.sub_pakets_id, items.banks_items_id,
concat(sp.code, ' ', sp.name) as sub_pakets_name, sp.order_num as sub_pakets_order_num, sp.is_active as sub_pakets_is_active,
p.id as pakets_id, concat(p.code, ' ', p.name) as pakets_name, p.order_num as pakets_order_num, p.is_active as pakets_is_active,
b.id as blocks_id, concat(b.code, ' ', b.name)  as blocks_name, b.order_num as blocks_order_num, b.is_active as blocks_is_active,
concat(bi.code, ' ', bi.name) as banks_items_name, bi.code as banks_items_code, bi.order_num as banks_items_order_num,
bi.is_active as banks_items_is_active
from items
left join banks_items bi
on bi.id = items.banks_items_id
left join sub_pakets sp
on sp.id = items.sub_pakets_id
left join pakets p
on p.id = sp.pakets_id
left join blocks b
on b.id = p.blocks_id
order by items.id;

select * from items_expanded;

create view contracts_expanded as
select
c.id, c.name, c.link, c.date,
CASE WHEN c.without_sum=TRUE THEN GREATEST(coalesce(a.acts_sum, 0), coalesce(p.payments_sum, 0)) ELSE c.sum END AS sum,
c.prepayment, c.payment_type, c.number, c.is_active, c.without_sum,
c.items_id, c.contractors_id, c.objects_id,
cr.name as contractors_name,
ie.name as items_name, ie.sub_pakets_name, ie.pakets_name, ie.blocks_name,
ie.banks_items_id, ie.banks_items_code, ie.banks_items_name,
obj.name as objects_name, obj.short_name as objects_short_name,
coalesce(a.acts_count, 0) as acts_count, coalesce(a.acts_sum, 0) as acts_sum,
coalesce(p.payments_count, 0) as payments_count, coalesce(p.payments_sum, 0) as payments_sum
from contracts c
left join contractors cr
on c.contractors_id = cr.id
left join (
select acts.contracts_id, sum(acts.sum) as acts_sum, count(acts.contracts_id) as acts_count
from acts
group by acts.contracts_id
) as a
on a.contracts_id = c.id
left join (
select payments.contracts_id, sum(payments.sum) as payments_sum, count(payments.contracts_id) as payments_count
from payments
group by payments.contracts_id
) as p
on p.contracts_id = c.id
left join objects obj
on c.objects_id = obj.id
left join items_expanded ie
on c.items_id = ie.id;

select * from contracts_expanded;

create view contractors_expanded as
select
cr.*,
contracts_sum, contracts_count,
acts_sum, acts_count,
payments_sum, payments_count
from contractors cr
left join (
select
contractors_id,
sum(sum) as contracts_sum, count(id) as contracts_count,
sum(acts_sum) as acts_sum, sum(acts_count) as acts_count,
sum(payments_sum) as payments_sum, sum(payments_count) as payments_count
from contracts_expanded
group by contractors_id
) as c
on cr.id = c.contractors_id
order by cr.id;

select * from contractors_expanded;

create view payments_expanded as
select
p.*,
b.name as banks_name,
concat('№ ', c.number, ' от ', DATE_FORMAT(c.date, '%d.%m.%Y'), ' || ', c.name) as contracts_name,
c.link as contracts_link, c.payment_type as contracts_payment_type,
c.contractors_id, c.contractors_name,
c.items_id, c.items_name, c.sub_pakets_name, c.pakets_name, c.blocks_name,
c.banks_items_id, c.banks_items_name,
c.objects_id, c.objects_short_name
from payments p
left join banks b
on b.id = p.banks_id
left join contracts_expanded c
on c.id = p.contracts_id;

select * from payments_expanded;

create view acts_expanded as
select
a.*, d.name as document_types_name,
concat('№ ', c.number, ' от ', DATE_FORMAT(c.date, '%d.%m.%Y'), ' || ', c.name) as contracts_name,
c.link as contracts_link,
c.contractors_id, c.contractors_name,
c.items_id, c.items_name,
c.sub_pakets_name, c.pakets_name, c.blocks_name,
c.banks_items_id, c.banks_items_name,
c.objects_id, c.objects_short_name
from acts a
left join document_types d
on d.id = a.document_types_id
left join contracts_expanded c
on c.id = a.contracts_id;

select * from acts_expanded;

create view reconciliation as
select
p.contractors_id, p.contractors_name,
p.contracts_id, p.contracts_name, p.contracts_link,
p.date, p.id as payments_id,
p.sum as payments_sum, null as acts_id, null as acts_sum
from payments_expanded p
union
select
a.contractors_id, a.contractors_name,
a.contracts_id, a.contracts_name, a.contracts_link,
a.date, null as payments_id, null as payments_sum,
a.id as acts_id, a.sum as acts_sum
from acts_expanded a
order by date;

select * from reconciliation;

create view contracts_balance as
select id,
concat('№ ', number, ' от ', DATE_FORMAT(date, '%d.%m.%Y'), ' || ', name) as name,
contractors_id, contractors_name, sum,
coalesce(payments_sum, 0) as payments_sum, coalesce(acts_sum, 0) as acts_sum,
coalesce(payments_sum, 0) - coalesce(acts_sum, 0) as debit, 0 as credit,
items_id, banks_items_id, blocks_name, pakets_name, sub_pakets_name, items_name
from contracts_expanded
where payment_type = 'Оплаты' and (payments_sum - acts_sum >= 0 or payments_sum - acts_sum is null)
union
select id,
concat('№ ', number, ' от ', DATE_FORMAT(date, '%d.%m.%Y'), ' || ', name) as name,
contractors_id, contractors_name, sum,
coalesce(payments_sum, 0) as payments_sum, coalesce(acts_sum, 0) as acts_sum,
0 as debit, coalesce(acts_sum, 0) - coalesce(payments_sum, 0) as credit,
items_id, banks_items_id, blocks_name, pakets_name, sub_pakets_name, items_name
from contracts_expanded
where payment_type = 'Оплаты' and (payments_sum - acts_sum < 0)
order by id;

select * from contracts_balance;

create view contractors_balance_auxiliary as
select
contractors_id as id, contractors_name as name, payment_type,
sum(sum) as contracts_sum, count(id) as contracts_count,
coalesce(sum(acts_sum), 0) as acts_sum, coalesce(sum(acts_count), 0) as acts_count,
coalesce(sum(payments_sum), 0) as payments_sum, coalesce(sum(payments_count), 0) as payments_count
from contracts_expanded c
group by contractors_id, payment_type
order by id;

select * from contractors_balance_auxiliary;

create view contractors_balance as
select
id, name, contracts_sum, payments_sum, acts_sum,
payments_sum - acts_sum as debit, 0 as credit
from contractors_balance_auxiliary
where payment_type = 'Оплаты' and (payments_sum - acts_sum >= 0 or payments_sum - acts_sum is null)
union
select 
id, name, contracts_sum, payments_sum, acts_sum,
0 as debit, acts_sum - payments_sum as credit
from contractors_balance_auxiliary
where payment_type = 'Оплаты' and (payments_sum - acts_sum < 0)
order by id;

select * from contractors_balance;
