/* Below is an annotated SQL program I wrote using AWS Redshift. Preceding this code, I created a churn model in excel. 
   This code allowed the client to productionalize that model by applying the scoring logic within their database to
   assess the probability of churn on a weekly basis. 

   This project was completed in 2016. Enhancements would include: creating the model in something more robust than
   Excel, likely using Python. I'd also prefer to utilize a SQL style guide for coding conventions. Since data storage
   is much cheaper now, with cloud datawarehouses, I would opt to keep historical churn scores in order to assess
   the performance of the model over time. Additionally, a version controlled data transformation tool such as dbt
   would provide an opportunity for peer feedback, thus eliminating the heavy use of comments for questions. */

--Region Active Subscriptions
-- Vars: Subscription_key, subscription_history_key, lob, business_unit, trading_name, account_number, account_key,

select a.subscription_key
	, c.account_key
	, e.src_account_id as account_number
	, b.business_unit
	, b.trading_name
	, d.lob
	, getdate() as listdate
into  retention_wkly_subs --drop table retention_wkly_subs
from fact_snp_subscription_activity_full a
join dim_business_unit_full b on a.business_unit_key = b.business_unit_key
join dim_subscription_full c on a.subscription_key = c.subscription_key
	and a.business_unit_key = c.business_unit_key
join dim_service_type_full d on c.service_type_key = d.service_type_key
join dim_account_history_full e on c.account_key = e.account_key
join dim_codes_full f on e.accounting_type_key = f.code_key
where to_date(a.reporting_date_key, 'yyyymmdd') between getdate()-7 and getdate() 
	and 		(
				(d.lob = 'Broadband' /*and d.service_type = 'ADSL'*/ ) --removed service_type 
				or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
				or (d.lob in('TV')) 
				or (d.lob='Mobile' and d.service_type='Mobile Postpaid')
				)
	and a.subscription_status = 'Active'
	and e.is_billable = 'Y'
	and e.is_current = 'Y'
	and f.code_description in ('Residential','Standard Residential','Standard')
group by a.subscription_key
	, c.account_key
	, e.src_account_id 
	, b.business_unit
	, b.trading_name
	, d.lob
having count(distinct a.reporting_date_key)=7;--Check this, I'm not sure that it's 7 or 6. Depends on when the fact_snp_subscription_activity table is updated. Also, it could be <7 if we're missing a day.
--23 min 11/28
--end region



--Region raw covariates
--Region Subscription Activity
--Vars: LOBs, Subscriptions, account_churns, sub_churns, account_tenure, sub_tenure,  Reporting days (from last 12 weeks)

select subs.*
, acct.account_setup
, lobs.subscriptions
, lobs.lobs
, subten.active_timestamp
, days.reportingdays
, churn1.account_churns
, churn2.sub_churns
into subvars
from retention_wkly_subs subs
left join (	select 
			x.account_key,
			min(e.account_setup_date) as account_setup
		from 
			 fact_snp_subscription_activity_full a 
			join dim_subscription_full c on a.subscription_key = c.subscription_key
				and a.business_unit_key = c.business_unit_key
			join dim_service_type_full d on c.service_type_key = d.service_type_key
			join dim_account_history_full e on c.account_key = e.account_key
			join dim_codes_full f on e.accounting_type_key = f.code_key
			join retention_wkly_subs x on c.account_key=x.account_key 
		where to_date(a.reporting_date_key, 'yyyymmdd')= getdate()-1 --Check: This assumes fact_snp_subscription_activity_full is current through yesterday
			and a.subscription_status = 'Active'
			and e.is_billable = 'Y'
			and e.is_current = 'Y'
			and (
				(d.lob = 'Broadband' and d.service_type = 'ADSL' )
				or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
				or (d.lob in('Mobile','TV')) 
				)
			and f.code_description in ('Residential','Standard Residential','Standard') 	
		group by 	
			x.account_key) acct on subs.account_key=acct.account_key
left join (		select c.account_key
					, count(distinct a.subscription_key) as subscriptions
					, count(distinct d.lob) as LOBs
				from fact_snp_subscription_activity_full a
				join dim_subscription_full c on a.subscription_key = c.subscription_key
					and a.business_unit_key = c.business_unit_key
				join dim_service_type_full d on c.service_type_key = d.service_type_key
				join dim_account_history_full e on c.account_key = e.account_key
				join dim_codes_full f on e.accounting_type_key = f.code_key
				where c.account_key in (select account_key from retention_wkly_subs)
					and a.reporting_date_key between  20151130 and 20170323 --Check: Do we want to change the end date each week? Not sure if the window should change or not.
					and 		(
								(d.lob = 'Broadband' and d.service_type = 'ADSL' )
								or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
								or (d.lob in('TV')) 
								or (d.lob='Mobile' and d.service_type='Mobile Postpaid')
								)
					and a.subscription_status = 'Active'
					and e.is_billable = 'Y'
					and e.is_current = 'Y'
					and f.code_description in ('Residential','Standard Residential','Standard')
				group by 
					  c.account_key
			) lobs on subs.account_key=lobs.account_key					
left join (	select 
			a.subscription_key,
			min(c.active_timestamp) as active_timestamp
			from fact_snp_subscription_activity_full a
			join dim_subscription_history_full c on a.subscription_key = c.subscription_key
				and a.business_unit_key = c.business_unit_key
			join dim_service_type_full d on c.service_type_key = d.service_type_key
			join dim_account_history_full e on c.account_key = e.account_key
			join dim_codes_full f on e.accounting_type_key = f.code_key
			join retention_wkly_subs x on c.subscription_key=x.subscription_key 
			where  to_date(a.reporting_date_key, 'yyyymmdd')= getdate()-1 --Check: Requires fact_snp_subscription_activity_full to be current through yesterday
			and	a.subscription_status = 'Active'
			and e.is_billable = 'Y'
			and e.is_current = 'Y'
			and (
				(d.lob = 'Broadband' and d.service_type = 'ADSL' )
				or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
				or (d.lob in('Mobile','TV')) 
				)
			and f.code_description in ('Residential','Standard Residential','Standard') 
			group by a.subscription_key) subten on subs.subscription_key=subten.subscription_key
left join (	select x.subscription_key
				, count(distinct a.reporting_date_key) as reportingdays
				from retention_wkly_subs x
				join fact_snp_subscription_activity_full a on x.subscription_key=a.subscription_key 
				join dim_subscription_full c on a.subscription_key = c.subscription_key
					and a.business_unit_key = c.business_unit_key
				join dim_service_type_full d on c.service_type_key = d.service_type_key
				join dim_account_history_full e on c.account_key = e.account_key
				join dim_codes_full f on e.accounting_type_key = f.code_key
				where to_date(a.reporting_date_key, 'yyyymmdd') between getdate()-84 and getdate() --12 weeks
					and 		(
								(d.lob = 'Broadband' and d.service_type = 'ADSL' )
								or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
								or (d.lob in('TV')) 
								or (d.lob='Mobile' and d.service_type='Mobile Postpaid')
								)
					and a.subscription_status = 'Active'
					and e.is_billable = 'Y'
					and e.is_current = 'Y'
					and f.code_description in ('Residential','Standard Residential','Standard')
				group by 
					 x.subscription_key
					) days on subs.subscription_key=days.subscription_key
left join 	(select a.account_key
			, count(b.churn_ind) as account_churns
			from retention_wkly_subs a 
			join 
				(select distinct
					e.account_key,
					a.reporting_date_key as churn_date_key,
					d.lob,
					1 as churn_ind
				from fact_snp_subscription_activity_full a
					join dim_reporting_classification_full b on a.reporting_classification_key = b.reporting_classification_key
					join dim_subscription_full c on a.subscription_key = c.subscription_key
						and a.business_unit_key = c.business_unit_key
					join dim_service_type_full d on c.service_type_key = d.service_type_key
					join dim_account_history_full e on c.account_key = e.account_key
					join dim_codes_full f on e.accounting_type_key = f.code_key
				where  e.account_key in (select account_key from retention_wkly_subs)
					and b.reporting_classification = 'Churn'
					and (
						(d.lob = 'Broadband' and d.service_type = 'ADSL' )
						or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
						or (d.lob in('TV')) 
						or (d.lob='Mobile' and d.service_type='Mobile Postpaid')
						)
					and f.code_description in ('Residential','Standard Residential','Standard')) b on a.account_key=b.account_key and to_date(b.churn_date_key, 'yyyymmdd') <=getdate() --Any Churns prior to week 
			group by a.account_key) churn1 on subs.account_key=churn1.account_key
left join 	(select a.subscription_key
			, count(b.churn_ind) as sub_churns
			from retention_wkly_subs a 
			join 
				(select distinct
					a.subscription_key,
					a.reporting_date_key as churn_date_key,
					d.lob,
					1 as churn_ind
				from fact_snp_subscription_activity_full a
					join dim_reporting_classification_full b on a.reporting_classification_key = b.reporting_classification_key
					join dim_subscription_full c on a.subscription_key = c.subscription_key
						and a.business_unit_key = c.business_unit_key
					join dim_service_type_full d on c.service_type_key = d.service_type_key
					join dim_account_history_full e on c.account_key = e.account_key
					join dim_codes_full f on e.accounting_type_key = f.code_key
				where a.subscription_key in (select subscription_key from retention_wkly_subs)
					and b.reporting_classification = 'Churn'
					and (
						(d.lob = 'Broadband' and d.service_type = 'ADSL' )
						or (d.lob = 'Fixed Voice' and d.service_type = 'Fixed Voice' )
						or (d.lob in('TV')) 
						or (d.lob='Mobile' and d.service_type='Mobile Postpaid')
						)
					and f.code_description in ('Residential','Standard Residential','Standard')) b on a.subscription_key=b.subscription_key and to_date(b.churn_date_key, 'yyyymmdd') <=a.listdate --Any Churns prior to week 
			group by a.subscription_key) churn2 on subs.subscription_key=churn2.subscription_key;
--51 min
--end region

--region Bill
--Vars: delinquent_flg, most recent bill date,total_delinquent_usd,delinquency_pct, due date of most recent bill,discounts,total_current_usd,adjustments
select *, case when delinquency_pct between 0.9 and 1.1 then 1 else 0 end as delinquent_flg
into bill --drop table bill;
from 
	(select
		b.account_key
		, row_number() over(partition by account_key order by bill_date_key desc) as row
		, c.bill_date_key as issued_date
		, c.due_date_key as due_date
		, sum(a.discounts_amount_local) as discounts
		, sum(a.adjustment_amount_local) as adjustments
		, sum((a.account_charges_amount_local + a.discounts_amount_local + a.service_charges_amount_local + a.adjustment_amount_local 
			+ a.tax_amount_local)*exch.exchange_rate) as total_current_USD
		, sum(a.outstanding_amount_local * exch.exchange_rate) as total_delinquent_USD
		, case when sum((a.account_charges_amount_local + a.discounts_amount_local + a.service_charges_amount_local + a.adjustment_amount_local 
			+ a.tax_amount_local)*exch.exchange_rate)=0 then 0 else 
		sum(a.outstanding_amount_local * exch.exchange_rate)/sum((a.account_charges_amount_local + a.discounts_amount_local + a.service_charges_amount_local + a.adjustment_amount_local 
			+ a.tax_amount_local)*exch.exchange_rate) end as delinquency_pct
		from fact_bill_full a
		join dim_account_history_full b on a.account_history_key = b.account_history_key
		join dim_bill_full c on a.bill_key = c.bill_key
		join fact_usd_exchange_rate_full exch on (c.bill_date_key = exch.date_key and a.currency_key = exch.currency_key)
		join dim_date d  on c.bill_date_key=d.date_key
	where account_key in (select account_key from retention_wkly_subs)
	group by 
	b.account_key
	, c.bill_date_key
	, c.due_date_key)
where row=1;
--7 min


--end region

--region usage
--Var: voice_sec_momentum, ,voice_cnt_momentum (calculated in next step)
--With statment starts by pulling all usage then filters into last month vs previous 2 months before last month so we can calculate momentum
with voice_usage as 
				(SELECT 
				dsub.subscription_key
				, fcdr.transaction_date_key
				-- Voice Calls -- 		
				,sum(case when fcdr.usage_type_key in (175,197,321,431,438,439) -- Usage_type = 'Voice' 
					then fcdr.units_count else 0 end) as voice_count_total	
				,sum(case when fcdr.usage_type_key in (175,197,321,431,438,439) -- Usage_type = 'Voice' 
					then fcdr.units_used else 0 end) as voice_seconds_total
			FROM fact_cdr_retail_summary_centriam_historical fcdr
				inner join dim_subscription_history_full dsub on fcdr.subscription_history_key = dsub.subscription_history_key
				inner join kb_oth_fv_inclusion x on dsub.subscription_key=x.subscription_key
				--inner join dim_business_unit_full bu on fcdr.business_unit_key = bu.business_unit_key
				--inner join dim_usage_type_full usg on usg.usage_type_key = fcdr.usage_type_key
				--inner join dim_date d on fcdr.transaction_date_key = d.date_key
			where dsub.subscription_key in (select subscription_key from retention_wkly_subs) 
			and fcdr.transaction_date_key >= 20161101 --Make sure this goes at least 12 weeks back.
			group by 
				dsub.subscription_key
				, fcdr.transaction_date_key),
last_month as (select subscription_key
				, sum(voice_count_total) as voice_count_last_month
				, sum(voice_seconds_total) as voice_sec_last_month
				from voice_usage a
				where to_date(transaction_date_key, 'yyyymmdd') between getdate()-28 and getdate()
				group by subscription_key),
Avg_2month as (
				select  subscription_key
				, count(transaction_date_key) as prior_Days
				, sum(voice_count_total) as voice_count
				, sum(voice_seconds_total) as voice_sec
				from voice_usage
				where to_date(transaction_date_key, 'yyyymmdd') between  getdate()-84 and getdate()-29
				group by subscription_key
				)
--These will be used when we finalize the metrics to create a voice count momentum and voiec sec momentum. The prior days determines the denominator for the prior months average.
select a.*, b.voice_count_last_month, c.prior_days, c.voice_count, b.voice_sec_last_month, c.voice_sec
into voice_usage --drop table voice_usage
from retention_wkly_subs a
left outer join last_month b on a.subscription_key=b.subscription_key  
left outer join avg_2month c on a.subscription_key=c.subscription_key ;
-- 5 min
--end region

--region Revenue
--Vars: total_rev, rev_last28

with rev1 as
			(
			/* Avg 3 Month Revenue by Subscription Type and Total */
			-- All Revenue
			SELECT
				sub.subscription_key,
			--	lob.lob,
			--	ser.service_type,
				sum (fchg.currency_amount_local) as Local_Amount,
				sum ((fchg.currency_amount_local * usd.exchange_rate)) as USD_Amount, 
				sum(case when to_date(fchg.transaction_date_key, 'yyyymmdd') between getdate()-28 and getdate() then (fchg.currency_amount_local * usd.exchange_rate) else 0 end) as rev28,
				sum(case when to_date(fchg.transaction_date_key, 'yyyymmdd') between getdate()-56 and getdate()-29 then (fchg.currency_amount_local * usd.exchange_rate) else 0 end) as rev56,
				sum(case when to_date(fchg.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()-57 then (fchg.currency_amount_local * usd.exchange_rate) else 0 end) as rev84,
				count (fchg.transaction_date_key) as Charge_Event_Count 
			FROM fact_charge_full   fchg -- view is not updated but has dates in where clause below
				join dim_charge_full dchg on dchg.charge_key = fchg.charge_key and dchg.charge_report_group <> 'Billed Discount included in charge'--
				join fact_usd_exchange_rate_full usd on usd.currency_key = fchg.currency_key 
					and usd.date_key = fchg.transaction_date_key 
			--	join dim_usage_type_full ust on ust.usage_type_key = fchg.usage_type_key
				join dim_subscription_history_full sub on sub.subscription_history_key = fchg.subscription_history_key
			--	join dim_service_type_full ser on ser.service_type_key = sub.service_type_key
				join dim_prepaid_postpaid_full p on p.prepaid_postpaid_key = fchg.prepaid_postpaid_key
			--	join dim_lob_full lob on lob.lob_key = fchg.lob_key
				join retention_wkly_subs x on sub.subscription_key=x.subscription_key and to_date(fchg.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()
			where p.is_postpaid = 'Y' 
				and (
					(fchg.lob_key = 2 and sub.service_type_key = 2)	--(lob.lob = 'Mobile' and ser.service_type in ('Mobile Postpaid'))
					or
					fchg.lob_key in (4,3,1)	--lob.lob in ('Broadband','Fixed Voice','TV')	
					)
			group by 		
				sub.subscription_key,
				--lob.lob,
				--ser.service_type
				),
				
				
rev2 as (
			SELECT
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type,
				sum(fcdr.currency_amount_local) as Local_Amount,
				sum(fcdr.currency_amount_local * exch.exchange_rate) as USD_Amount,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-28 and getdate() then (fcdr.currency_amount_local * exch.exchange_rate) else 0 end) as rev28,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-56 and getdate()-29 then (fcdr.currency_amount_local * exch.exchange_rate) else 0 end) as rev56,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()-57 then (fcdr.currency_amount_local * exch.exchange_rate) else 0 end) as rev84,
				count (fcdr.transaction_date_key) as Charge_Event_Count  
			FROM fact_cdr_retail_summary_centriam_historical fcdr 
				inner join dim_subscription_history_full dsub on fcdr.subscription_history_key = dsub.subscription_history_key
				inner join dim_bu_bill_cycle_code_full dob on dob.bu_bill_cycle_code_key = fcdr.bu_bill_cycle_code_key
			--	inner join dim_service_type_full ser on dsub.service_type_key = ser.service_type_key
				inner join fact_usd_exchange_rate_full exch ON fcdr.currency_key = exch.currency_key 
					and dob.bill_date_key = exch.date_key
			--	inner join dim_usage_type_full usg on usg.usage_type_key = fcdr.usage_type_key
				inner join dim_subscription_full dsub2 on dsub.subscription_key = dsub2.subscription_key
				join retention_wkly_subs x on dsub2.subscription_key=x.subscription_key and to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()
			where  fcdr.source_type_key = 1 --	source_type = 'Billed'
			group by 
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type
			), 

rev3 as (
			-- Prepaid cdr_retail 
			select 
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type,
				sum(fcdr.currency_amount_local) as Local_Amount,
				sum(fcdr.currency_amount_local * usd.exchange_rate) as USD_Amount,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-28 and getdate() then (fcdr.currency_amount_local * usd.exchange_rate) else 0 end) as rev28,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-56 and getdate()-29 then (fcdr.currency_amount_local * usd.exchange_rate) else 0 end) as rev56,
				sum(case when to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()-57 then (fcdr.currency_amount_local * usd.exchange_rate) else 0 end) as rev84,
				count (fcdr.transaction_date_key) as Charge_Event_Count 
			from fact_cdr_retail_summary_centriam_historical fcdr
				join fact_usd_exchange_rate_full usd ON fcdr.currency_key = usd.currency_key
					and fcdr.transaction_date_key = usd.date_key
				join dim_subscription_history_full dsub ON fcdr.subscription_history_key = dsub.subscription_history_key
				join dim_subscription_full dsub2 on dsub.subscription_key = dsub2.subscription_key
			--	join dim_usage_type_full usg on usg.usage_type_key = fcdr.usage_type_key
			--	join dim_service_type_full ser on dsub.service_type_key = ser.service_type_key
				join retention_wkly_subs x on dsub2.subscription_key=x.subscription_key and to_date(fcdr.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()
			where fcdr.source_type_key = 3 -- 'Rated' type key 3
			group by 
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type
			),
		
rev4 as (
			-- Prepaid Bolton 
			select 
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type,
				sum(a.currency_amount_local) as Local_Amount,
				sum((a.currency_amount_local * usd.exchange_rate)*-1) as USD_Amount,
				sum(case when to_date(a.transaction_date_key, 'yyyymmdd') between getdate()-28 and getdate() then (a.currency_amount_local * usd.exchange_rate) else 0 end) as rev28,
				sum(case when to_date(a.transaction_date_key, 'yyyymmdd') between getdate()-56 and getdate()-29 then (a.currency_amount_local * usd.exchange_rate) else 0 end) as rev56,
				sum(case when to_date(a.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()-57 then (a.currency_amount_local * usd.exchange_rate) else 0 end) as rev84,
				count (1) as Charge_Event_Count  
			from fact_bolton_sale_full a -- view not updated, but has dates from where clause below
				join fact_usd_exchange_rate_full usd ON a.currency_key = usd.currency_key
					and a.transaction_date_key = usd.date_key
				join dim_subscription_history_full dsub ON a.subscription_history_key = dsub.subscription_history_key
					AND dsub.business_unit_key = a.business_unit_key ------------------ other join between these tables in Seamus' code did not have this "and" clause
				join dim_subscription_full dsub2 on dsub.subscription_key = dsub2.subscription_key
			--	join dim_usage_type_full usg on usg.usage_type_key = a.usage_type_key
			--	join dim_service_type_full ser on dsub.service_type_key = ser.service_type_key
				join retention_wkly_subs x on dsub2.subscription_key=x.subscription_key and to_date(a.transaction_date_key, 'yyyymmdd') between getdate()-84 and getdate()		
			group by 
				dsub2.subscription_key,
			--	ser.lob,
			--	ser.service_type
			)


select a.subscription_key
--	, sum(usd_amount) as total_rev
	, sum(isnull(b.usd_amount, 0) + isnull(c.usd_amount, 0) + isnull(d.usd_amount, 0) + isnull(e.usd_amount, 0)) as total_rev
--	, sum(rev28)/((sum(rev56)+sum(rev84))/2)
	, sum(isnull(b.rev28, 0) + isnull(c.rev28, 0) + isnull(d.rev28, 0) + isnull(e.rev28, 0)) as rev_last28
	, sum(isnull(b.rev56, 0) + isnull(c.rev56, 0) + isnull(d.rev56, 0) + isnull(e.rev56, 0)) as rev_2monthsprior
	, sum(isnull(b.rev84, 0) + isnull(c.rev84, 0) + isnull(d.rev84, 0) + isnull(e.rev84, 0)) as rev_3monthsprior
into revenue --drop table revenue
from retention_wkly_subs a
left join rev1 b on a.subscription_key=b.subscription_key 
left join rev2 c on a.subscription_key=c.subscription_key 
left join rev3 d on a.subscription_key=d.subscription_key 
left join rev4 e on a.subscription_key=e.subscription_key 
group by a.subscription_key;
--38 min
--end region

--region Product
-- Vars: product_cnt
select d.subscription_key, count(distinct b.product_key) as product_cnt
into products
from fact_product_full b
join dim_product c on b.product_key = c.product_key
join dim_subscription_history_full d on b.subscription_history_key=d.subscription_history_key
where d.subscription_key in (select subscription_key from retention_wkly_subs)
group by d.subscription_key;
-- 1 min
--end region 

--end region

--Region Finalize Covariates

select a.*
, datediff(day, b.account_setup, getdate()) as account_tenure  
, datediff(day, b.active_timestamp, getdate()) as sub_tenure  
, b.subscriptions
, b.lobs
, b.reportingdays
, b.account_churns
, b.sub_churns
, c.total_delinquent_usd
, c.discounts
, c.total_current_usd
, c.adjustments
, case when c.total_current_usd<=0 then 0 else --set those that are 0 or negative to 0, if they have a credit they shouldn't be delinquent. Can't divide by 0.
	(case when c.total_delinquent_usd<0 then 0 else c.total_delinquent_usd end)/c.total_current_usd end as delinquency_pct --Negative delinquency is unused credit, not really delinquent
, case when ( case when c.total_current_usd<=0 then 0 else --set those that are 0 or negative to 0, if they have a credit they shouldn't be delinquent. Can't divide by 0.
			(case when c.total_delinquent_usd<0 then 0 else c.total_delinquent_usd end)/c.total_current_usd end) between 0.9 and 1.1 then 1 else 0 end as delinquent_flg
, datediff(day, to_date(c.issued_date, 'yyyymmdd'), getdate()) as days_since_bill
, datediff(day, getdate(), to_date(c.due_date, 'yyyymmdd')) as days_until_due
, case when d.prior_months is null or (cast(d.voice_count as float)/ cast(d.prior_months as float))=0 then null else cast(d.voice_count_last_month as float)/(cast(d.voice_count as float)/ cast(d.prior_months as float)) end as voice_cnt_momentum
, case when d.prior_months is null or (cast(d.voice_sec as float)/ cast(d.prior_months as float))=0 then null else cast(d.voice_sec_last_month as float)/(cast(d.voice_sec as float)/ cast(d.prior_months as float))end as voice_sec_momentum
, e.total_rev
, e.rev_last28
, f.product_cnt
, (case when b.subscription_key is null then 1 end+
		case when c.account_key is null then 1 end+
		case when d.subscription_key is null then 1 end+
		case when e.subscription_key is null then 1 end+
		case when f.subscription_key is null then 1 end) as missing
into retention_raw
from retention_wkly_subs a
left join subvars b on a.subscription_key=b.subscription_key
left join bill c on a.account_key=c.account_key
left join (select *
			, case when prior_days between 28 and 56 then 2
				when prior_days between 1 and 27 then 1
				else null end as prior_months
			from voice_usage) d on a.subscription_key=d.subscription_key
left join revenue e on a.subscription_key=e.subscription_key
left join products f on a.subscription_key=f.subscription_key;


--1min

--Reassign Nulls and create: 
select a.subscription_key
, a.account_key
, a.account_number
, a.business_unit
, a.trading_name
, a.lob
, a.listdate
, a.reportingdays
, isnull(isnull(a.delinquency_pct, b.avg_delinquency_pct), c.avg_delinquency_pct) as delinquency_pct --if null, average of that market. if still null, then overall avg (all markets)
, isnull(a.delinquent_flg, 0) as delinquent_flg
, isnull(a.discounts,0) as discounts
, isnull(a.adjustments,0) as adjustments
, isnull(a.total_current_usd,0) as total_current_usd
, isnull(isnull(a.product_cnt, b.avg_product_cnt), c.avg_product_cnt) as product_cnt
, isnull(isnull(a.product_cnt, b.avg_product_cnt), c.avg_product_cnt) as product_cnt2 --same as above- null=average
, isnull(a.product_cnt, 0) as product_cnt_0 --make Nulls 0
, a.lobs
, a.subscriptions
, isnull(isnull(a.days_since_bill, b.avg_days_since_bill), c.avg_days_since_bill) as days_since_bill --if null, average of that market. if still null, then overall avg (all markets)
, isnull(isnull(a.days_until_due, b.avg_days_until_due), c.avg_days_until_due) as days_until_due --if null, average of that market. if still null, then overall avg (all markets)
, isnull(isnull(a.total_delinquent_usd, b.avg_total_delinquent_usd), c.avg_total_delinquent_usd) as total_delinquent_usd --if null, average of that market. if still null, then overall avg (all markets)
, isnull(a.account_churns, 0) as account_churns
, isnull(a.sub_churns, 0) as sub_churns
, a.account_tenure
, a.sub_tenure
, isnull(isnull(a.voice_cnt_momentum, b.avg_voice_cnt_momentum), c.avg_voice_cnt_momentum) as voice_cnt_momentum --if null, average of that market. if still null, then overall avg (all markets)
, isnull(isnull(a.voice_cnt_momentum, b.avg_voice_cnt_momentum), c.avg_voice_cnt_momentum) as voice_cnt_momentum_avg --Same as voice_cnt_momentum just different name
, isnull(isnull(a.voice_sec_momentum, b.avg_voice_sec_momentum), c.avg_voice_sec_momentum) as voice_sec_momentum --if null, average of that market. if still null, then overall avg (all markets)
, case when voice_cnt_momentum >50 then 50 else voice_cnt_momentum end as voice_cnt_momentum_adj
, case when a.total_rev= 0 then b.avg_total_rev else a.total_rev end as total_rev
, case when a.rev_last28= 0 then b.avg_rev_last28 else a.rev_last28 end as rev_last28
, (case when a.rev_last28= 0 then b.avg_rev_last28 else a.rev_last28 end)/b.avg_rev_last28 as rev_index
, a.missing
, d.avg_rev_last28 as rev_last28_avg
, d.avg_total_rev as MRR_BU
, e.line_subscr_cnt
, case when  e.line_subscr_cnt>1 then 1 else 0 end as mult_line_subscr_flg
, f.lob_mix
into retention_covars --drop table retention_covars
from retention_raw a
join (select business_unit
		, avg(cast (delinquency_pct as float)) as avg_delinquency_pct
		, avg(cast(product_cnt as float)) as avg_product_cnt
		, avg(cast(days_since_bill as float)) as avg_days_since_bill
		, avg(cast(days_until_due as float)) as avg_days_until_due
		, avg(cast(total_delinquent_usd as float)) as avg_total_delinquent_usd
		, avg(cast(voice_cnt_momentum as float)) as avg_voice_cnt_momentum
		, avg(cast(voice_sec_momentum as float)) as avg_voice_sec_momentum
		, avg(cast(total_rev as float)) as avg_total_rev
		, avg(cast(rev_last28 as float)) as avg_rev_last28
		from retention_raw
		group by business_unit) b on a.business_unit=b.business_unit 
join (select listdate
		, avg(cast (delinquency_pct as float)) as avg_delinquency_pct
		, avg(cast(product_cnt as float)) as avg_product_cnt
		, avg(cast(days_since_bill as float)) as avg_days_since_bill
		, avg(cast(days_until_due as float)) as avg_days_until_due
		, avg(cast(total_delinquent_usd as float)) as avg_total_delinquent_usd
		, avg(cast(voice_cnt_momentum as float)) as avg_voice_cnt_momentum
		, avg(cast(voice_sec_momentum as float)) as avg_voice_sec_momentum
		from retention_raw
		group by listdate
		) c on a.listdate=c.listdate --not really a join, just forcing it to join to itself
join (select business_unit
		, avg(cast(total_rev as float)) as avg_total_rev
		, avg(cast(rev_last28 as float)) as avg_rev_last28
		from retention_raw
		group by business_unit) d on a.business_unit=d.business_unit
join (select account_key, lob, count(distinct subscription_key) as line_subscr_cnt
		from retention_raw
		group by account_key, lob) e on a.account_key=e.account_key and a.lob=e.lob
/*LOB Mix*/		
join (select a.subscription_key
		, case when a.lob='Fixed Voice' then f.lob||isnull(b.LOB,'')||isnull(t.LOB,'')
			when a.lob='Broadband' then b.lob||isnull(f.LOB,'')||isnull(t.LOB,'')
			when a.lob='TV' then t.lob||isnull(b.LOB,'')||isnull(f.LOB,'')
		end as lob_mix
		from retention_raw a
		left join (select distinct account_key, lob from retention_raw where lob='Fixed Voice') f on a.account_key=f.account_key
		left join (select distinct account_key, lob from retention_raw where lob='Broadband') b	on a.account_key=b.account_key
		left join (select distinct account_key, lob from retention_raw where lob='TV') t on a.account_key=t.account_key
		) f on a.subscription_key=f.subscription_key;
--<1min		
--end region


--region Score FV
select *
, case when business_unit='Jamaica' then (exp(-5.4083 +
											(discounts * -0.00043) +
											(account_churns * 0.53950) +
											(total_delinquent_usd * 0.0006) +
											(account_tenure * -0.00003) +
											(voice_cnt_momentum_adj * -0.1341) +
											(case when lob_mix='Fixed VoiceBroadband' then 0.1649 else 0 end))
										/(1+exp(-5.4083 +
											(discounts * -0.00043) +
											(account_churns * 0.53950) +
											(total_delinquent_usd * 0.0006) +
											(account_tenure * -0.00003) +
											(voice_cnt_momentum_adj * -0.1341) +
											(case when lob_mix='Fixed VoiceBroadband' then 0.1649 else 0 end))))

		when business_unit='Trinidad' then (exp(-1.4739 +
											(subscriptions * -1.12740) +
											(account_churns * -0.15350) +
											(sub_tenure * -0.00045) +
											(voice_cnt_momentum_avg * -0.01160) +
											(rev_last28_avg * 0.0003780) +
											(case when lob_mix='Fixed VoiceBroadband' then 0.6586
												when lob_mix='Fixed VoiceBroadbandTV' then 1.8839
												when lob_mix='Fixed VoiceTV' then 0.773300 else 0 end)+
											(case when missing = 2 then 0.3588 else 0 end) +
											(case when mult_line_subscr_flg = 1 then 0.988300 else 0 end))
										/(1+exp(-1.4739 +
											(subscriptions * -1.12740) +
											(account_churns * -0.15350) +
											(sub_tenure * -0.00045) +
											(voice_cnt_momentum_avg * -0.01160) +
											(rev_last28_avg * 0.0003780) +
											(case when lob_mix='Fixed VoiceBroadband' then 0.6586
												when lob_mix='Fixed VoiceBroadbandTV' then 1.8839
												when lob_mix='Fixed VoiceTV' then 0.773300 else 0 end)+
											(case when missing = 2 then 0.3588 else 0 end) +
											(case when mult_line_subscr_flg = 1 then 0.988300 else 0 end))) )
		
		when business_unit in ('Barbados', 'Cayman') then (exp(-3.4574+
															(case when business_unit = 'Cayman' then -0.6883 else 0 end) +
															(product_cnt * 0.0298) +
															(total_delinquent_usd *0.000393)+
															(account_churns * 0.1341) +
															(account_tenure * -0.00006) +
															(voice_cnt_momentum * -0.3157) +
															(case when lob_mix='Fixed VoiceBroadband' then 0.3646
																 when lob_mix= 'Fixed VoiceBroadbandTV' then 0.3955
																 when lob_mix=	'Fixed VoiceTV' then 0.8481
															else 0 end) +
															(case when missing = 1 then -0.4021
																when missing = 2 then -0.6897 else 0 end))
														/(1+exp(-3.4574+
															(case when business_unit = 'Cayman' then -0.6883 else 0 end) +
															(product_cnt * 0.0298) +
															(total_delinquent_usd *0.000393)+
															(account_churns * 0.1341) +
															(account_tenure * -0.00006) +
															(voice_cnt_momentum * -0.3157) +
															(case when lob_mix='Fixed VoiceBroadband' then 0.3646
																 when lob_mix= 'Fixed VoiceBroadbandTV' then 0.3955
																 when lob_mix=	'Fixed VoiceTV' then 0.8481
															else 0 end) +
															(case when missing = 1 then -0.4021
																when missing = 2 then -0.6897 else 0 end))))
		else (exp(-5.4877 +
				(delinquency_pct*0.00175) +
				(case when lobs=2 then 0.6586
					when lobs=3 then 0.519
					when lobs=4 then 1.233
					else 0 end)+
				(days_until_due*0.00298) +
				(total_delinquent_usd*0.000039) +
				(account_tenure*sub_tenure*-0.0000000128) +
				(voice_sec_momentum*0.000565) +
				(rev_index*-0.2885)+
				(case when lob_mix='Fixed VoiceBroadband' then -1.0228
					when lob_mix='Fixed VoiceBroadbandTV' then -0.8422
					when lob_mix='Fixed VoiceTV' then -0.8504 else 0 end)+
				--(bu_churn_rate*122.6) +
				(mrr_bu*0.00414))
			/(1+exp(-5.4877 +
				(delinquency_pct*0.00175) +
				(case when lobs=2 then 0.6586
					when lobs=3 then 0.519
					when lobs=4 then 1.233
					else 0 end)+
				(days_until_due*0.00298) +
				(total_delinquent_usd*0.000039) +
				(sub_tenure*-0.00018) +
				(voice_sec_momentum*0.000565) +
				(rev_index*-0.2885)+
				(case when lob_mix='Fixed VoiceBroadband' then -1.0228
					when lob_mix='Fixed VoiceBroadbandTV' then -0.8422
					when lob_mix='Fixed VoiceTV' then -0.8504 else 0 end)+
				--(bu_churn_rate*122.6) +
				(mrr_bu*0.00414))))
end as fv_score
into retention_fv_score --drop table retention_fv_score
from retention_covars
where lob='Fixed Voice' and (business_unit<>'Jamaica' or (business_unit='Jamaica' and trading_name='Lime')); --exclude Jamaica Flow

-- end region
--region Score BB
--Make sure to add fv_score and fv_subs
select a.*
, isnull(b.fv_score, 0) as fv_score
, isnull(b.fv_subs, 0) as fv_subs
, case when business_unit='Jamaica' then (exp(-3.8898+
											(total_delinquent_usd*0.00375)+
											(product_cnt2*0.2568)+ 
											(case when lob_mix='BroadbandFixed Voice' then -0.4032
												 when lob_mix= 'BroadbandFixed VoiceTV' then -0.8555
												 when lob_mix=	'BroadbandTV' then -0.364
											else 0 end)+
											(days_since_bill*0.00681)+
											(fv_score*-105.6)+
											(total_current_usd*-0.0219)+
											(discounts*0.000448)+
											(adjustments*0.000177)+
											(reportingdays*-0.00346)+
											(sub_churns*-0.1237)+
											(sub_tenure*-0.00001))
										/(1+exp(-3.8898+
											(total_delinquent_usd*0.00375)+
											(product_cnt2*0.2568)+ 
											(case when lob_mix='BroadbandFixed Voice' then -0.4032
												 when lob_mix= 'BroadbandFixed VoiceTV' then -0.8555
												 when lob_mix=	'BroadbandTV' then -0.364
											else 0 end)+
											(days_since_bill*0.00681)+
											(fv_score*-105.6)+
											(total_current_usd*-0.0219)+
											(discounts*0.000448)+
											(adjustments*0.000177)+
											(reportingdays*-0.00346)+
											(sub_churns*-0.1237)+
											(sub_tenure*-0.00001))))

		when business_unit='Trinidad' then (exp(-2.9460 +
											(product_cnt * -0.11540) +
											(subscriptions * -0.05310) +
											(account_churns * 0.0282000) +
											(account_tenure * -0.00011) +
											(sub_tenure * -0.0002) +
											(rev_last28_avg * -0.005750) +
											(case when lob_mix='BroadbandFixed Voice' then -0.317400
												when lob_mix='BroadbandFixed VoiceTV' then -0.110200
												when lob_mix='BroadbandTV' then 0.350000 else 0 end)+
											(case when mult_line_subscr_flg = 1 then -0.964900 else 0 end)+
											(fv_score * 13.895000))
										/(1+exp(-2.9460 +
											(product_cnt * -0.11540) +
											(subscriptions * -0.05310) +
											(account_churns * 0.0282000) +
											(account_tenure * -0.00011) +
											(sub_tenure * -0.0002) +
											(rev_last28_avg * -0.005750) +
											(case when lob_mix='BroadbandFixed Voice' then -0.317400
												when lob_mix='BroadbandFixed VoiceTV' then -0.110200
												when lob_mix='BroadbandTV' then 0.350000 else 0 end)+
											(case when mult_line_subscr_flg = 1 then -0.964900 else 0 end)+
											(fv_score * 13.895000))))
		
		when business_unit in ('Barbados', 'Cayman') then (exp(-2.7757+
															(case when business_unit = 'Cayman' then -0.7543 else 0 end) +
															(delinquent_flg*-0.1894) +
															(total_delinquent_usd *0.000414)+
															(account_churns * 0.3111) +
															(account_tenure * -0.00005) +
															(fv_subs*-0.5547)+
															(case when lob_mix='BroadbandFixed Voice' then 0.1978
															when lob_mix= 'BroadbandFixed VoiceTV' then 0.2293
															when lob_mix=	'BroadbandTV' then 0.2399
															else 0 end) +
															(case when missing = 1 then -0.6323
																when missing = 2 then -1.0292 else 0 end))
														/(1+exp(-2.7757+
															(case when business_unit = 'Cayman' then -0.7543 else 0 end) +
															(delinquent_flg*-0.1894) +
															(total_delinquent_usd *0.000414)+
															(account_churns * 0.3111) +
															(account_tenure * -0.00005) +
															(fv_subs*-0.5547)+
															(case when lob_mix='BroadbandFixed Voice' then 0.1978
															when lob_mix= 'BroadbandFixed VoiceTV' then 0.2293
															when lob_mix=	'BroadbandTV' then 0.2399
															else 0 end) +
															(case when missing = 1 then -0.6323
																when missing = 2 then -1.0292 else 0 end))))
		else (exp(-3.8208 +
		(case when lobs=2 then 2.0275
			when lobs=3 then 2.0374
			when lobs=4 then 1.3969
			else 0 end) +
		(subscriptions*0.0379) +
		(total_delinquent_usd*0.00022) +
		(sub_churns*0.6093) +
		(account_tenure*-0.00003) +
		(sub_tenure*-0.00026)+
		(rev_last28*-0.00072)+
		(case when lob_mix='BroadbandFixed Voice' then -2.0822
			when lob_mix='BroadbandFixed VoiceTV' then -2.2156
			when lob_mix='BroadbandTV' then -1.9302 
		else 0 end)+
		--(bu_churn_rate*-21.9577) +
		(mrr_bu*0.00218)+
		(fv_subs*-0.3316)+
		(fv_score*6.164)
		) 
	/(1+exp(-3.8208 +
		(case when lobs=2 then 2.0275
			when lobs=3 then 2.0374
			when lobs=4 then 1.3969
			else 0 end)+
		(subscriptions*0.0379) +
		(total_delinquent_usd*0.00022) +
		(sub_churns*0.6093) +
		(account_tenure*-0.00003) +
		(sub_tenure*-0.00026)+
		(rev_last28*-0.00072)+
		(case when lob_mix='BroadbandFixed Voice' then -2.0822
			when lob_mix='BroadbandFixed VoiceTV' then -2.2156
			when lob_mix='BroadbandTV' then -1.9302 else 0 end)+
		--(bu_churn_rate*-21.9577) +
		(mrr_bu*0.00218)+
		(fv_subs*-0.3316)+
		(fv_score*6.164)
		))) 
end as bb_score
into retention_bb_score
from retention_covars a
left join (select account_key, count(distinct subscription_key) as fv_subs, avg(cast(fv_score as float)) as fv_score
			from retention_fv_score
			group by account_key) b on a.account_key=b.account_key
where a.lob='Broadband';
--end region
--region Score TV
-- Make sure to add fv_score, fv_subs, bb_score, and bb_subs
select a.*
, isnull(b.fv_score, 0) as fv_score
, isnull(b.fv_subs, 0) as fv_subs
, isnull(c.bb_score, 0) as bb_score
, isnull(c.bb_subs, 0) as bb_subs
, case when business_unit='Jamaica' then (exp(-4.1705+
											(product_cnt_0*-0.00378)+--product_cnt2 
											(case when lob_mix='TVBroadband' then 0.8854
												 when lob_mix= 'TVBroadbandFixed Voice' then 0.3311
												 when lob_mix=	'TVFixed Voice' then -0.2077
											else 0 end)+
											(bb_score*-27.5366)+
											(reportingdays*0.00353)+
											(sub_churns*0.4435)+
											(account_tenure*-0.00021))
										/(1+exp(-4.1705+
											(product_cnt_0*-0.00378)+--product_cnt2 
											(case when lob_mix='TVBroadband' then 0.8854
												 when lob_mix= 'TVBroadbandFixed Voice' then 0.3311
												 when lob_mix=	'TVFixed Voice' then -0.2077
											else 0 end)+
											(bb_score*-27.5366)+
											(reportingdays*0.00353)+
											(sub_churns*0.4435)+
											(account_tenure*-0.00021))))

		when business_unit='Trinidad' then ( exp(-2.9489 +
												(product_cnt * -0.02370) +
												(subscriptions * -1.02170) +
												(account_churns * 0.0587000) +
												(sub_churns * -0.78800) +	
												(account_tenure * -0.00014) +
												(sub_tenure * -0.00013) +
												(rev_last28_avg * -0.004420) +
												(case when lob_mix='TVBroadband' then 1.159900
													when lob_mix='TVBroadbandFixed Voice' then 1.864300
													when lob_mix='TVFixed Voice' then 0.747300 else 0 end)+
												(line_subscr_cnt * 1.148000) +
												(fv_score * 7.809900) +
												(bb_score * 3.321500))
											/(1+exp(-2.9489 +
												(product_cnt * -0.02370) +
												(subscriptions * -1.02170) +
												(account_churns * 0.0587000) +
												(sub_churns * -0.78800) +	
												(account_tenure * -0.00014) +
												(sub_tenure * -0.00013) +
												(rev_last28_avg * -0.004420) +
												(case when lob_mix='TVBroadband' then 1.159900
													when lob_mix='TVBroadbandFixed Voice' then 1.864300
													when lob_mix='TVFixed Voice' then 0.747300 else 0 end)+
												(line_subscr_cnt * 1.148000) +
												(fv_score * 7.809900) +
												(bb_score * 3.321500))))
		
		when business_unit in ('Barbados', 'Cayman') then (exp(-2.0677+
															(case when business_unit = 'Cayman' then -1.309 else 0 end) +
															(case when lobs=2 then 1.259
																 when lobs=3 then 2.4148
																 when lobs=4 then 2.1936
															else 0 end) +
															(account_churns *0.2922)+
															(sub_tenure * -0.00046) +
															(fv_score * 13.5186) +
															(bb_score * -6.8324) +
															(case when lob_mix='TVBroadband' then -1.1556
																 when lob_mix= 'TVBroadbandFixed Voice' then -3.2846
																 when lob_mix=	'TVFixed Voice' then -2.1433
															else 0 end) +
															(missing *-0.4375))
														/(1+ exp(-2.0677+
															(case when business_unit = 'Cayman' then -1.309 else 0 end) +
															(case when lobs=2 then 1.259
																 when lobs=3 then 2.4148
																 when lobs=4 then 2.1936
															else 0 end) +
															(account_churns *0.2922)+
															(sub_tenure * -0.00046) +
															(fv_score * 13.5186) +
															(bb_score * -6.8324) +
															(case when lob_mix='TVBroadband' then -1.1556
																 when lob_mix= 'TVBroadbandFixed Voice' then -3.2846
																 when lob_mix=	'TVFixed Voice' then -2.1433
															else 0 end) +
															(missing *-0.4375))))
		else (exp(-2.3429 +
				(case when business_unit= 'Anguilla' then -1.1131
							when business_unit= 'Antigua' then 0.5838
							when business_unit= 'Curacao' then -0.4801
							when business_unit= 'Grenada' then 0.1307
							when business_unit= 'St Vincent' then 0.063
							when business_unit= 'Turks' then -2.6994
					else 0 end) +
				(delinquent_flg*-0.9433) +
				(product_cnt*-0.042) +
				(sub_churns*0.6093) +
				(case when lobs= 2 then 0.6591
					when lobs= 3 then 1.7848
					when lobs= 4 then 2.7384
				else 0 end) +
				(subscriptions*-0.0877)+
				(days_since_bill*0.00278)+
				(account_churns*0.0545)+
				(sub_churns*-0.1721)+
				(account_tenure*-0.00008)+
				(sub_tenure*-0.00038)+
				(total_rev*0.00455)+
				(rev_last28*-0.02)+
				(case when lob_mix= 'TVBroadband' then -0.7428
					when lob_mix= 'TVBroadbandFixed Voice' then -2.3965
					when lob_mix= 'TVFixed Voice' then -1.0989
				else 0 end)+
				(case when missing= 1 then -0.7522
					when missing= 2 then -0.8983
					else 0 end)+
				--(rev28_bu_wkly*0.0113) +
				(bb_score*7.0298)
				) 
			/(1+exp(-2.3429 +
				(case when business_unit= 'Anguilla' then -1.1131
							when business_unit= 'Antigua' then 0.5838
							when business_unit= 'Curacao' then -0.4801
							when business_unit= 'Grenada' then 0.1307
							when business_unit= 'St Vincent' then 0.063
							when business_unit= 'Turks' then -2.6994
					else 0 end) +
				(delinquent_flg*-0.9433) +
				(product_cnt*-0.042) +
				(sub_churns*0.6093) +
				(case when lobs= 2 then 0.6591
					when lobs= 3 then 1.7848
					when lobs= 4 then 2.7384
				else 0 end) +
				(subscriptions*-0.0877)+
				(days_since_bill*0.00278)+
				(account_churns*0.0545)+
				(sub_churns*-0.1721)+
				(account_tenure*-0.00008)+
				(sub_tenure*-0.00038)+
				(total_rev*0.00455)+
				(rev_last28*-0.02)+
				(case when lob_mix= 'TVBroadband' then -0.7428
					when lob_mix= 'TVBroadbandFixed Voice' then -2.3965
					when lob_mix= 'TVFixed Voice' then -1.0989
				else 0 end)+
				(case when missing= 1 then -0.7522
					when missing= 2 then -0.8983
					else 0 end)+
				--(rev28_bu_wkly*0.0113) +
				(bb_score*7.0298)
				))) 
end as tv_score
into retention_tv_score
from retention_covars a
left join (select account_key, count(distinct subscription_key) as fv_subs, avg(cast(fv_score as float)) as fv_score
			from retention_fv_score
			group by account_key) b on a.account_key=b.account_key
left join (select account_key, count(distinct subscription_key) as bb_subs, avg(cast(bb_score as float)) as bb_score
			from retention_bb_score
			group by account_key) c on a.account_key=c.account_key;

--end region

--region Merge scores
--Get the list of all accounts/LOBs
with base as (select distinct account_key, business_unit,max(lobs) as lobs from
										 (select account_key, business_unit, lobs
									     from retention_fv_score
									     union
									     select account_key,  business_unit, lobs
									     from retention_bb_score
									     union
									     select account_key, business_unit, lobs
									     from retention_tv_score
												)
				group by account_key, business_unit
				),
--region Grab Revenue stats from original modeling set before scoring (some records get excluded during scoring)'
fv_rev as (
	select account_key, business_unit, count(Subscription_key) as fv_subs, --max(churn_ind) as fv_Churn_ind,
			sum(rev_last28) as fv_rev_last28,sum(total_rev) as fv_total_rev
    from retention_covars
	where lob='Fixed Voice'
    group by account_key,  business_unit
	),
/*27s*/	
bb_rev as (
	select account_key,  business_unit, count(Subscription_key) as bb_subs, --max(churn_ind) as bb_Churn_ind,
			sum(rev_last28) as bb_rev_last28,sum(total_rev) as bb_total_rev
    from retention_covars
	where lob='Broadband'
    group by account_key, business_unit
	),
	
tv_rev as (
	select account_key, business_unit, count(Subscription_key) as tv_subs, --max(churn_ind) as tv_Churn_ind,
			sum(rev_last28) as tv_rev_last28,sum(total_rev) as tv_total_rev
    from retention_covars
	where lob='TV'
    group by account_key, business_unit

	),
	--end region
	
--region Get the model scores
fv_scores as (
	select account_key, business_unit, avg(cast(fv_score as float)) as avg_fv_score
	from retention_fv_score    
	where fv_score is not null
    group by account_key,business_unit
	),

bb_scores as (
	select account_key, business_unit, avg(cast(bb_score as float)) as avg_bb_score
    from retention_bb_score
    where bb_score is not null
    group by account_key, business_unit
	),
	
tv_scores as (
	select account_key, business_unit, avg(cast(tv_score as float)) as avg_tv_score
    from retention_tv_score
    where tv_score is not null
    group by account_key, business_unit
	)
--end region

select distinct
base.account_key, base.business_unit,base.lobs,
fv.fv_subs,FVs.avg_fv_score,  fv.fv_rev_last28, fv.fv_total_rev/3 as fv_mrr,
bb.bb_subs,BBs.avg_bb_score,  bb.bb_rev_last28, bb.bb_total_rev/3 as bb_mrr,
tv.tv_subs,TVs.avg_tv_score,  tv.tv_rev_last28, tv.tv_total_rev/3 as tv_mrr,
(case when fv.fv_subs is not null then 1 else 0 end)+(case when bb.bb_subs is not null then 1 else 0 end)+(case when tv.tv_subs is not null then 1 else 0 end) as Scored_Lobs,
(case when fv_rev_last28 is null then 0 else fv_rev_last28 end)+(case when bb_rev_last28 is null then 0 else bb_rev_last28 end)+
	(case when tv_rev_last28 is null then 0 else tv_rev_last28 end) as acct_rev_last28,
(case when fv_total_rev is null then 0 else fv_total_rev/3 end)+(case when bb_total_rev is null then 0 else bb_total_rev/3 end)+
	(case when tv_total_rev is null then 0 else tv_total_rev/3 end) as account_mrr,		
(case when avg_fv_score is null then 0 else avg_fv_score end)+(case when avg_bb_score is null then 0 else avg_bb_score end)+
	(case when avg_tv_score is null then 0 else avg_tv_score end) as acct_score_sum,
--case when fv.fv_Churn_ind =1 OR bb.bb_Churn_ind = 1 OR tv.tv_Churn_ind=1 then 1 else 0 end as acct_churn_ind,
case when isnull(FVs.avg_fv_score,0) > isnull(BBs.avg_bb_score,0) and isnull(FVs.avg_fv_score,0) > isnull(TVs.avg_tv_score,0) then FVs.avg_fv_score
	 when isnull(BBs.avg_bb_score,0) > isnull(FVs.avg_fv_score,0) and isnull(BBs.avg_bb_score,0) > isnull(TVs.avg_tv_score,0) then BBs.avg_bb_score
	 when isnull(TVs.avg_tv_score,0) > isnull(FVs.avg_fv_score,0) and isnull(TVs.avg_tv_score,0) > isnull(BBs.avg_bb_score,0) then TVs.avg_tv_score else null end as max_score,
case when isnull(FVs.avg_fv_score,0) > isnull(BBs.avg_bb_score,0) and isnull(FVs.avg_fv_score,0) > isnull(TVs.avg_tv_score,0) then 'FV'
	 when isnull(BBs.avg_bb_score,0) > isnull(FVs.avg_fv_score,0) and isnull(BBs.avg_bb_score,0) > isnull(TVs.avg_tv_score,0) then 'BB'
	 when isnull(TVs.avg_tv_score,0) > isnull(FVs.avg_fv_score,0) and isnull(TVs.avg_tv_score,0) > isnull(BBs.avg_bb_score,0) then 'TV' else null end as max_score_source
--ntile(20) over(partition by base.weekend order by max_score desc) as icosatile,
--ntile(4635) over(partition by base.weekend order by max_score desc) as percentile
into retention_scores_merged
from base as base
	LEFT JOIN fv_rev AS FV on base.account_key = fv.account_key and base.business_unit = fv.business_unit
	LEFT JOIN bb_rev AS BB on base.account_key = bb.account_key and base.business_unit = bb.business_unit
	LEFT JOIN tv_rev AS TV on base.account_key = tv.account_key and base.business_unit = tv.business_unit
	LEFT JOIN fv_scores AS FVs on base.account_key = FVs.account_key and base.business_unit = FVs.business_unit
	LEFT JOIN bb_scores AS BBs on base.account_key = BBs.account_key and base.business_unit = BBs.business_unit
	LEFT JOIN TV_scores AS TVs on base.account_key = TVs.account_key and base.business_unit = TVs.business_unit
where avg_fv_score is not null or avg_bb_score is not null or avg_tv_score is not null
;

--end region

--region Determine List Inclusion
--From scored list, multiply max score x account mrr and rank
--Join to the weekly list pull table that has all the covariates and the flags for detractors/excessive callers?
select *
, max_score*account_mrr as churn_score
, rank() over(order by max_score*account_mrr desc) as churn_score_rank
from retention_scores_merged a



--end region

--region add additional list pull variables
-- Vars: Rule flags? , LOB Flags, revenue vars, contact phone numbers, last_identified, last_included
--end region

-- region Apply Suppression logic
-- end region

--region Randomize control group and assign treatment
--end region

--region insert into final retention table
--end region


/*Drop staging tables*/
-- drop table retention_wkly_subs;
-- drop table subvars;
-- drop table bill;
-- drop table voice_usage;
-- drop table revenue;
-- drop table products;
-- drop table retention_raw;
-- drop table retention_covars;
-- drop table retention_fv_score;
-- drop table retention_bb_score;
-- drop table retention_tv_score;
--drop table retention_scores_merged;
