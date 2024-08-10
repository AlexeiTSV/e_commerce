-- Ссылка на датасет: https://www.kaggle.com/datasets/carrie1/ecommerce-data
-- Ссылка на дашборд: https://datalens.yandex/k9jpaatejaty7

-- подготовка таблицы для импорта данных из csv
create table e_commerce_data_raw
(
	InvoiceNo text,
	StockCode text,
	Description text,
	Quantity int,
	InvoiceDate text,
	Unitprice numeric,
	CustomerId int,
	Country text
);

-- импорт данных из csv
copy e_commerce_data_raw from 'C:\localpath\data.csv' delimiter ',' csv header encoding 'WIN1252';

-- подготовка рабочего датасета
create table e_commerce_data
as
	select 
		invoiceno
		,stockcode
		,description
		,quantity
		,to_timestamp(invoicedate, 'MM/DD/YYYY HH24:MI')::timestamp as invoicedate
		,unitprice
		,customerid
		,country
	from e_commerce_data_raw
	where left(stockcode, 1) ~ '^[0-9\.]+$' is true -- оставляю строки, начинающиеся с цифр
		and quantity > 0 -- оставляю положительные значения кол-ва товаров
		and to_timestamp(invoicedate, 'MM/DD/YYYY HH24:MI')::date between '2011-01-01' and '2011-11-30' -- оставляю только завершенные месяца 2011г.
		and customerid is not null; -- оставляю только заполненные customerid	


	-- 1. Удержание	

-- 1.1. Сохранение покупателей с течением времени (по периодам)
select 
	"period"
	,first_value(cohort_retained) over (order by "period") as cohort_size
	,cohort_retained
	,round(cohort_retained * 100.0 / first_value(cohort_retained) over (order by "period"), 2) 
		as pct_retained
from
(
	-- формирование когорт покупателей
	select 
		date_part('month', age(ECM2.invoicedate, ECM1.first_order)) as "period"
		,count(distinct ECM1.customerid) as cohort_retained
	from (
			-- покупатели и дата их первого заказа
			select 
				customerid
				,min(invoicedate) as first_order	
			from e_commerce_data			
			group by 1	
			
		) ECM1
	join e_commerce_data ECM2 on ECM2.customerid = ECM1.customerid
	group by 1	
) cohorts;	

	
-- 1.2. Когорты, полученные из временного ряда
-- Сохранение покупателей с течением времени (по периодам) в разбивке по когортам на основе месяца первой покупки
select
	first_month
	,"period"
	,first_value(cohort_retained) over (partition by first_month order by "period") as cohort_size
	,cohort_retained
	,round(cohort_retained * 100.0 / first_value(cohort_retained) over (partition by first_month order by "period"), 2)
		as pct_retained
from 
(
	-- через какие периоды времени (в месяцах) когорты совершали покупки
	select
		date_part('month', ECD1.first_order) as first_month
		,date_part('month', age(ECD2.invoicedate, ECD1.first_order)) as "period"
		,count(distinct ECD1.customerid) as cohort_retained
	from (
			-- покупатели и дата их первого заказа
			select 
				customerid
				,min(invoicedate) as first_order	
			from e_commerce_data			
			group by 1
		) ECD1
	join e_commerce_data ECD2 on ECD2.customerid = ECD1.customerid
	group by 1, 2
) cohorts;
	
	
	-- 2. Выживаемость 
	
-- 2.1. Выживаемость когорт (на основе первой покупки) через один месяц
select
	first_month
	,count(customerid) as cohort_size
	-- кол-во покупателей, чья "жизнь" составляет один месяц и более
	,count(case when lifespan >= 1 then customerid end) as survived_1 
	,round(count(case when lifespan >= 1 then customerid end) * 100.0 / count(customerid), 2)	
		as pct_survived_1
from
(
	select 
		customerid
		,date_part('month', min(invoicedate)) as first_month
		-- период (в месяцах) между первой и последней покупками
		,date_part('month', age(max(invoicedate), min(invoicedate))) as lifespan
	from e_commerce_data
	group by 1
) customers_lifespan
group by 1
order by 1;


-- 2.2. Количество покупателей, совершивших 1, 2 ... 10 покупок, в разбивке по когортам (на основе месяца первой покупки)
select
	ECD.cohort_month
	-- список количества заказов от 1 до 10 
	,GS.invoices
	,count(distinct customerid) as cohort
	-- количество покупателей (в когорте) совершивших 1, 2 ... 10 покупок
	,count(distinct case when ECD.total_orders >= GS.invoices then customerid end) as cohort_survived
	,round(
			count(distinct case when ECD.total_orders >= GS.invoices then customerid end) * 100.0
			/ count(distinct customerid)
		, 2) as pct_survived
from
(
	select 
		customerid
		-- распределение покупателей на когорты на основе первого заказа
		,date_part('month', min(invoicedate)) as cohort_month
		,min(invoicedate) as first_order
		-- количество уникальных заказов
		,count(distinct invoiceno) as total_orders
	from e_commerce_data
	group by 1
) ECD

join -- cross join таблицы покупателей и общим количеством их заказов с таблицей заказов от 1 до 10  

(	-- таблица с заказами от 1 до 10
	select generate_series as invoices
	from generate_series(1,10,1)
) GS

on 1 = 1
group by 1, 2;


	-- 3. Возвращаемость
	
-- 3.1. покупатели вернувшиеся в течение указанных периодов времени (от 1 до 4х недель)
with cte1 as
(
	-- распределение покупателей на когорты на основе первого заказа
	select
		date_part('month', ECD1.first_order) as cohort_month
		,count(customerid) as customers_in_cohort
	from
	(
		select 
			customerid
			,min(invoicedate) as first_order
		from e_commerce_data
		group by 1
	) ECD1
	group by 1
),
cte2 as 
(
	select 
		date_part('month', ECD1.first_order) as cohort_month
		
		-- покупатели совершившие вторую покупку в течение указанных периодов времени
		,count(distinct case when age(ECD2.invoicedate, ECD1.first_order) <= interval '7 days' then ECD1.customerid end) 
			as customers_returned_1week
		
		,count(distinct case when age(ECD2.invoicedate, ECD1.first_order) <= interval '14 days' then ECD1.customerid end) 
			as customers_returned_2weeks			
			
		,count(distinct case when age(ECD2.invoicedate, ECD1.first_order) <= interval '21 days' then ECD1.customerid end) 
			as customers_returned_3weeks			
			
		,count(distinct case when age(ECD2.invoicedate, ECD1.first_order) <= interval '28 days' then ECD1.customerid end) 
			as customers_returned_4weeks			
	from
	(
		select 
			customerid
			,min(invoicedate) as first_order
		from e_commerce_data
		group by 1
	) ECD1
	join e_commerce_data ECD2 on ECD2.customerid = ECD1.customerid
		-- оставляем покупателей с двумя и более покупками
		and ECD2.invoicedate > ECD1.first_order
	group by 1
)
select
	cte1.cohort_month
	,cte1.customers_in_cohort
	
	-- количество покупателей вернувшихся в течение 1...4 недель
	,cte2.customers_returned_1week
	,cte2.customers_returned_2weeks
	,cte2.customers_returned_3weeks	
	,cte2.customers_returned_4weeks	
	
	-- процент вернувшихся в течение 1...4 недель
	,round(cte2.customers_returned_1week * 100.0 / cte1.customers_in_cohort, 2) as pct_returned_1week
	,round(cte2.customers_returned_2weeks * 100.0 / cte1.customers_in_cohort, 2) as pct_returned_2weeks	
	,round(cte2.customers_returned_3weeks * 100.0 / cte1.customers_in_cohort, 2) as pct_returned_3weeks	
	,round(cte2.customers_returned_4weeks * 100.0 / cte1.customers_in_cohort, 2) as pct_returned_4weeks	
from cte1 
left join cte2 on cte2.cohort_month = cte1.cohort_month
-- убираю когорту 11 месяца, 
-- т.к. не у всех покупателей, впервые совершивших покупку в течение этого месяца, 
-- было время (1...4 недели) на совершение еще покупок
where cte1.cohort_month <> 11;


	-- 4. Поперечный анализ через все когорты 
	
-- 4.1. Когортный состав покупателей в каждом месяце
select 
	months
	,first_order
	,count(distinct ECD1.customerid) as customers
	
	-- формирование когорт (в оконной функции вычисляется сумма уникальных единиц пользователей)
	,sum(count(distinct ECD1.customerid)) over (partition by months) as cohort
	
	,round(
			count(distinct ECD1.customerid) * 100.0 /
			sum(count(distinct ECD1.customerid)) over (partition by months) 
		,2) as pct_month
from 
(
	-- покупатели и месяц, в котором они совершили первую покупку
	select 
		customerid
		,date_part('month', min(invoicedate)) as first_order
	from e_commerce_data
	group by 1
) ECD1

join

(
	-- покупатели и все месяцы, в которых они совершали покупки
	select 
		customerid
		,date_part('month', invoicedate) as months
	from e_commerce_data
	group by 1, 2
) ECD2

on ECD2.customerid = ECD1.customerid
group by 1, 2;


-- 4.2. такой запрос что и в п.4.1., но подготовленный для вывода на график дашборда
select 
	months
	
	-- вычисляем когортный состав (в %) покупателей по каждому месяцу 
	,round(
			count(distinct case when first_order = 1 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_1
	

	,round(
			count(distinct case when first_order = 2 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_2				
				
	,round(
			count(distinct case when first_order = 3 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_3
	

	,round(
			count(distinct case when first_order = 4 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_4				
				
	,round(
			count(distinct case when first_order = 5 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_5
	

	,round(
			count(distinct case when first_order = 6 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_6				
				
				
	,round(
			count(distinct case when first_order = 7 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_7
	

	,round(
			count(distinct case when first_order = 8 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_8				
				
	,round(
			count(distinct case when first_order = 9 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_9
	

	,round(
			count(distinct case when first_order = 10 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_10				

	,round(
			count(distinct case when first_order = 11 then ECD1.customerid end) * 100.0 / 
			count(distinct ECD1.customerid)
		,2) as pct_11	
from 
(
	-- покупатели и месяц, в котором они совершили первую покупку
	select 
		customerid
		,date_part('month', min(invoicedate)) as first_order
	from e_commerce_data
	group by 1
) ECD1

join

(
	-- покупатели и все месяцы, в которых они совершали покупки
	select 
		customerid
		,date_part('month', invoicedate) as months
	from e_commerce_data
	group by 1, 2
) ECD2

on ECD2.customerid = ECD1.customerid
group by 1;
	
