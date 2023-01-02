-- Представления в БД калькулятора

-- Цены материалов могут не содержать объекта, на который они были запрошены,
-- это значит, что эти цены актуальны на все объекты. Множим эту строчку цены на каждый объект
create view materials_price_history_full as
select mph.*
from materials_prices_history mph
where not isnull(objects_id)
union
select
mph.id, materials_id, materials_invoice_name, contractors_id, o.id as objects_id, date, price, payment_method
from materials_prices_history mph
cross join objects o
where isnull(objects_id);

select * from materials_price_history_full;

-- Здесь выбираем цены за 15 дней с последнего запроса на каждый материал
create view recent_materials_prices as
select mph.*, d.min_date
from materials_price_history_full mph
left join(
select materials_id, objects_id, 
DATE_SUB(max(date), INTERVAL 15 DAY) as min_date
from materials_price_history_full
group by materials_id, objects_id
) as d
on d.materials_id = mph.materials_id and d.objects_id = mph.objects_id
where mph.date >= d.min_date;

-- Представление возвращает материалы с их минимальными ценами и поставщиками
-- за интервал в 15 дней с последнего запроса цены
create view materials_actual_prices_auto as
SELECT
rmp.objects_id, rmp.materials_id,
rmp.id as materials_prices_history_id,
rmp.contractors_id, rmp.date, rmp.price, rmp.payment_method,
o.name as objects_name, o.short_name as objects_short_name,
m.name as materials_name, rmp.materials_invoice_name, m.ed_izm, m.price_control,
m.material_types_id, mt.name as material_types_name,
rmp.contractors_id, c.name as contractors_name, c.inn as contractors_inn, c.contacts as contractors_contacts
FROM recent_materials_prices rmp INNER JOIN (
	SELECT id, materials_id, objects_id, MIN(price) AS min_price
	FROM recent_materials_prices
	GROUP BY materials_id, objects_id
) t ON rmp.materials_id = t.materials_id AND rmp.objects_id = t.objects_id AND rmp.price = t.min_price
left join objects o
on o.id = rmp.objects_id
left join contractors c
on c.id = rmp.contractors_id
left join materials m
on m.id = rmp.materials_id
left join material_types mt
on mt.id = m.material_types_id
;

create view contracts_expanded as
select c.*, obj.short_name as objects_short_name, cr.name as contractors_name
from contracts c
left join contractors cr
on cr.id = c.contractors_id
left join objects obj
on obj.id = c.objects_id;

select * from contracts_expanded;

create view items_expanded as
select i.id, concat(i.clc_code, ' ', i.name) as name, i.order_num, i.is_active,
i.sub_pakets_id, i.banks_items_id,
sp.name as sub_pakets_name, sp.order_num as sub_pakets_order_num,
sp.is_active as sub_pakets_is_active, sp.pakets_id
from items i
left join sub_pakets sp
on i.sub_pakets_id = sp.id;

select * from items_expanded;

-- Соединяем цены материалов, определенные автоматически с теми, что заданы вручную
create view materials_actual_prices_expanded as
select ap.*,
mat.name,
date, price, payment_method, ph.contractors_id,
cr.name as contractors_name, cr.contacts as contractors_contacts
from materials_actual_prices ap
left join prices_history ph
on ph.id = ap.prices_history_id
left join contractors cr
on cr.id = ph.contractors_id
left join materials mat
on mat.id = ap.materials_id;

select * from actual_prices_expanded;

-- Строим расширенную таблицу основных материалов для работ в калькуляции
create view r_ek_basic_materials_expanded as
select r_ek.*,
m.name as materials_name, m.ed_izm as materials_ed_izm,
r_mat.consumption_rate,
ap.price, ap.contractors_name, ap.contractors_contacts,
coalesce(coalesce(r_ek.closed_overconsumption, oc.overconsumption), 1) as true_overconsumption,
coalesce(coalesce(r_ek.closed_overconsumption, oc.overconsumption), 1) * r_mat.consumption_rate as true_consumption_rate,
coalesce(coalesce(r_ek.closed_overconsumption, oc.overconsumption), 1) * r_mat.consumption_rate
* ek.volume as true_volume,
coalesce(r_ek.closed_price, ap.price) as true_price,
coalesce(coalesce(r_ek.closed_overconsumption, oc.overconsumption), 1) * r_mat.consumption_rate
* ek.volume * coalesce(r_ek.closed_price, ap.price) as total_cost,
coalesce(coalesce(r_ek.closed_overconsumption, oc.overconsumption), 1) * r_mat.consumption_rate
* ek.volume * ap.price as total_cost_without_supply,
ek.clc_id, ek.acts_id, ek.closed_status, ek.work_types_id,
clc.objects_id
from r_ek_basic_materials r_ek
left join materials m
on m.id = r_ek.materials_id
left join ek
on ek.id = r_ek.ek_id
left join clc
on clc.id = ek.clc_id
left join overconsumption oc
on clc.objects_id = oc.objects_id and ek.work_types_id = oc.work_types_id and
r_ek.materials_id = oc.materials_id
left join r_work_types_basic_materials r_mat
on r_mat.materials_id = r_ek.materials_id and r_mat.work_types_id = ek.work_types_id
left join actual_prices_expanded ap
on ap.materials_id = r_ek.materials_id and ap.objects_id = clc.objects_id;

select * from r_ek_basic_materials_expanded;

-- Строим расширенную таблицу вспомогательных материалов для работ в калькуляции
create view r_ek_add_materials_expanded as
select r_ek.*,
m.name as materials_name, m.ed_izm as materials_ed_izm,
ap.price, ap.contractors_name, ap.contractors_contacts,
coalesce(r_ek.closed_price, ap.price) as true_price,
r_ek.volume * coalesce(r_ek.closed_price, ap.price) as total_cost,
ek.clc_id, ek.acts_id, ek.closed_status, ek.work_types_id,
clc.objects_id
from r_ek_add_materials r_ek
left join materials m
on m.id = r_ek.materials_id
left join ek
on ek.id = r_ek.ek_id
left join clc
on clc.id = ek.clc_id
left join actual_prices_expanded ap
on ap.materials_id = r_ek.materials_id and ap.objects_id = clc.objects_id;

select * from r_ek_add_materials_expanded;

-- Строим расширенную таблицу работ в калькуляции
create view ek_expanded as
select ek.*,
ep.name as ep_name,
wt.name as work_types_name,
wt.ed_izm as work_types_ed_izm, wt.unit_price as work_types_unit_price,
coalesce(ek.closed_price, wt.unit_price) as ek_volume_unit_work_cost,
(ek.volume * coalesce(ek.closed_price, wt.unit_price)) as work_types_total_cost,
coalesce(add_mat.add_materials_total_cost, 0) as add_materials_total_cost,
coalesce(basic_mat.basic_materials_total_cost, 0) as basic_materials_total_cost,
coalesce(basic_mat.basic_materials_total_cost_without_supply, 0) as basic_materials_total_cost_without_supply,
coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost, 0)
as all_materials_total_cost,
coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost_without_supply, 0)
as all_materials_total_cost_without_supply,
coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost, 0)
+ (ek.volume * coalesce(ek.closed_price, wt.unit_price)) as ek_total_cost,
coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost_without_supply, 0)
+ (ek.volume * coalesce(ek.closed_price, wt.unit_price)) as ek_total_cost_without_supply,
(coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost, 0) +
(ek.volume * coalesce(ek.closed_price, wt.unit_price))) / ek.volume as ek_volume_unit_total_cost,
(coalesce(add_mat.add_materials_total_cost, 0) + coalesce(basic_mat.basic_materials_total_cost_without_supply, 0) +
(ek.volume * coalesce(ek.closed_price, wt.unit_price))) / ek.volume as ek_volume_unit_total_cost_without_supply
from ek
left join ep
on ep.id = ek.ep_id
left join work_types wt
on wt.id = ek.work_types_id
left join(
select ek_id, sum(total_cost) as add_materials_total_cost
from r_ek_add_materials_expanded
group by ek_id
) as add_mat
on add_mat.ek_id = ek.id
left join(
select ek_id, sum(total_cost) as basic_materials_total_cost,
sum(total_cost_without_supply) as basic_materials_total_cost_without_supply
from r_ek_basic_materials_expanded
group by ek_id
) as basic_mat
on basic_mat.ek_id = ek.id;

select * from ek_expanded;

-- Строим расширенную таблицу расчетов
create view estimations_expanded as
select estimations.*,
et.name as estimation_types_name,
it.name as items_name,
obj.short_name as objects_short_name, obj.name as objects_name,
obj.full_name as objects_full_name,
ek.work_types_volume,
coalesce(ek.work_types_total_cost, 0) as work_types_total_cost,
coalesce(ek.all_materials_total_cost, 0) as all_materials_total_cost,
coalesce(ek.all_materials_total_cost, 0) + coalesce(ek.work_types_total_cost, 0)
as est_total_cost,
coalesce(ek.work_types_total_cost, 0) / ek.work_types_volume
as est_volume_unit_work_cost,
(coalesce(ek.all_materials_total_cost, 0) + coalesce(ek.work_types_total_cost, 0)) /
ek.work_types_volume as est_volume_unit_total_cost
from estimations est
left join(
select estimations_id,
sum(volume) as work_types_volume,
sum(all_materials_total_cost) as all_materials_total_cost,
sum(work_types_total_cost) as work_types_total_cost
from ek_expanded
group by estimations_id
) as ek
on ek.estimations_id = est.id
left join estimation_types et
on et.id = est.estimation_types_id
left join items_expanded it
on it.id = est.items_id
left join objects obj
on obj.id = est.objects_id;

select * from clc_expanded;

