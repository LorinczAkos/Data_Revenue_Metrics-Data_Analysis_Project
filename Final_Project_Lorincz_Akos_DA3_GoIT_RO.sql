with monthly_revenue as (
select 
	date(date_trunc('month', payment_date)) as payment_month,
	user_id,
	game_name,
	sum(revenue_amount_usd) as total_revenue
from project.games_payments gp
group by 1,2,3
),
revenue_lag_lead_months as (
	select
	*,
	date(payment_month - interval '1' month) as previous_calendar_month,
	date(payment_month + interval '1' month) as next_calendar_month,
	lag(total_revenue) over(partition by user_id order by payment_month) as previous_paid_month_revenue,
	lag(payment_month) over(partition by user_id order by payment_month) as previous_paid_month,
	lead(payment_month) over(partition by user_id order by payment_month) as next_paid_month
from monthly_revenue
),
revenue_metrics as (
	select
	payment_month,
	user_id,
	game_name,
	total_revenue,
	case
		when previous_paid_month is null
		then total_revenue
	end as new_mrr,
	case
		when next_paid_month is null
		or next_paid_month != next_calendar_month
		then next_calendar_month
	end churn_month,
	case 
		when next_paid_month is null
		or next_paid_month != next_calendar_month
		then total_revenue
	end as churned_revenue,
	case
		when previous_paid_month = previous_calendar_month
		and total_revenue > previous_paid_month_revenue
		then total_revenue - previous_paid_month_revenue
	end as expansion_revenue,
	case
		when previous_paid_month = previous_calendar_month
		and total_revenue < previous_paid_month_revenue
		then total_revenue - previous_paid_month_revenue 
	end as contraction_revenue
from revenue_lag_lead_months
)
select
	rm.*,
	gpu.language,
	gpu.has_older_device_model,
	gpu.age
from revenue_metrics rm
left join project.games_paid_users gpu using(user_id)


