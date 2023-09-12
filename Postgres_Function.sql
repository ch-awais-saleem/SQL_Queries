Postgres Function 



In the code snippet you've provided, there are several options and configurations that can be customized when defining a PostgreSQL function. These options can affect the behavior and properties of the function. Here are some common options and configurations:
Function Parameters: You can define input parameters for your function by specifying them within parentheses after the function name. For example:
–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
CREATE OR REPLACE FUNCTION my_function(param1 INT, param2 TEXT)
RETURNS void
LANGUAGE plpgsql
AS $$
-- Function body goes here
$$
––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––

Parameters allow you to pass values into the function and use them in your SQL logic.
Security Definer: You can specify whether the function runs with the privileges of the user who defines it (the default) or with the privileges of the user who calls it. This is controlled by the SECURITY option:
SECURITY DEFINER: The function runs with the privileges of the user who defines it.
SECURITY INVOKER: The function runs with the privileges of the user who calls it (default).
Immutable, Stable, or Volatile: The VOLATILE keyword in your code snippet indicates the volatility of the function. Functions can be categorized as immutable, stable, or volatile based on whether they return the same result for the same input arguments and database state. Other options include:
IMMUTABLE: The function always produces the same result for the same input arguments and database state.
STABLE: The function produces the same result for the same input arguments within a single SQL statement.
VOLATILE: The function can produce different results for the same input arguments and database state (as in your example).
Returns Data Type: You can specify the data type that the function returns after the RETURNS keyword.
Error Handling: You can include error handling logic using BEGIN ... EXCEPTION blocks to catch and handle exceptions that might occur during the execution of your function.
Language: While your example uses PL/pgSQL as the language, PostgreSQL supports other procedural languages like PL/Python, PL/Perl, and PL/Tcl. The choice of language depends on your familiarity and the specific requirements of your function.
Custom Configuration Parameters: You can set custom configuration parameters for your function using the SET statement within the function body to control aspects of its behavior.
Security and Access Control: You can use GRANT and REVOKE statements to control who can execute or modify the function.
Dependency Management: You can specify other database objects (e.g., tables, views, functions) that your function depends on using the DEPENDS ON option.
Cost and Performance Optimization: You can add query optimization hints using COST and ROWS options to help the query planner make better execution plan choices for your function

COST 10  -- Set the cost to 10 points
SECURITY INVOKER  -- Set SECURITY INVOKER - by the permissions allowed to invoker of function
SECURITY DEFINER  -- Set SECURITY INVOKER - by the permissions allowed to definer of function

–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
CREATE OR REPLACE FUNCTION dbo.populate_rpt_zone_brch_cust_agnt_wallet_name()
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
begin
	
	truncate table dbo.rpt_zone_brch_cust_agnt_wallet_name;
	
	insert into dbo.rpt_zone_brch_cust_agnt_wallet_name
	("year",
	month_name,
   zone_name,
   brch_name,
	business_type,
   wallet_id,
   full_name,
	no_of_transactions_in_current_month,
	no_of_transactions_in_last_month,
   sum_of_transactions_in_current_month,
   sum_of_transactions_in_last_month,
	growth_in_no_of_transactions,
	growth_in_sum_of_transactions
	)
	
	select
       year::int,
  		 trim(month_name),
       zone_name,
       brch_name,
       business_type,
       walt_acct_nmbr as wallet_id,
       full_name,
	    no_of_transactions_in_current_month,
	    no_of_transactions_in_last_month,
	    sum_of_transactions_in_current_month,
	    sum_of_transactions_in_last_month,
	  	round((((no_of_transactions_in_current_month - no_of_transactions_in_last_month)::float / (case (no_of_transactions_in_last_month) when 0 then 1 else (no_of_transactions_in_last_month) end)::float) * 100)::numeric,2) as growth_in_no_of_transactions,
	  	round((((sum_of_transactions_in_current_month - sum_of_transactions_in_last_month)::float / (case (sum_of_transactions_in_last_month) when 0 then 1 else (sum_of_transactions_in_last_month) end)::float) * 100)::numeric,2) as growth_in_sum_of_transactions
	from (
	
	select  extract(year from act.tran_date) as "year",
	        extract(month from act.tran_date) as monthid,
	        TO_CHAR(dc.bm_created_on, 'Month') as month_name,
           zone_name as zone_name,
   		brch_name as brch_name,
	        case when prod_name='Haseb wallet product' then 'Haseb' when prod_name='Agent wallet product' then 'Agent' else null end as  business_type,
	        walt_acct_nmbr,
	        dc.agnt_full_name as full_name,
			coalesce (count(distinct act.bsns_prtn_id),0) as no_of_transactions_in_current_month,
			
			coalesce (lag(count(distinct act.bsns_prtn_id)) over (partition by prod_name, extract(year from act.tran_date) order by extract(month from act.tran_date)),0) as no_of_transactions_in_last_month,
			coalesce (sum(distinct act.bsns_prtn_id),0) as sum_of_transactions_in_current_month,
			
			coalesce (lag(sum(distinct act.bsns_prtn_id)) over (partition by prod_name, extract(year from act.tran_date) order by extract(month from act.tran_date)),0) as sum_of_transactions_in_last_month	
			
			from dbo.dim_agent dc
	    	inner join dbo.acct_tran act on
	    	dc.bp_main_id =act.bsns_prtn_id
	where  	amnt_type_code='00000'
			and sub_oprn_type='00001'
			and prod_id in (select prod_id  from dbo.prod where prod_name in ('Haseb wallet product','Agent wallet product'))
	--and dc.bp_main_id =35587
	--and mfloos_spclst_name is not null
	group by prod_name, 1, 2, 3,4,5,6,7,8
	
	union all
	
	select  extract(year from act.tran_date) as "year",
	        extract(month from act.tran_date) as monthid,
	        TO_CHAR(dc.bm_created_on, 'Month') as month_name,
          zone_name as zone_name,
   		brch_name as brch_name,
	        case dc.bsns_type_id
	        	when (select code from dbo.bsns_acct_type where code='00001') then 'Individual'
	        	when (select code from dbo.bsns_acct_type where code='00002') then 'Business'
	        	end as business_type,
	        walt_acct_nmbr,
	        bp_full_name as full_name,
			coalesce (count(distinct act.bsns_prtn_id),0) as no_of_transactions_in_current_month,
			
			coalesce (lag(count(distinct act.bsns_prtn_id)) over (partition by dc.bsns_type_id, extract(year from act.tran_date) order by extract(month from act.tran_date)),0) as no_of_transactions_in_last_month,
			coalesce (sum(distinct act.bsns_prtn_id),0) as sum_of_transactions_in_current_month,
			
			coalesce (lag(sum(distinct act.bsns_prtn_id)) over (partition by dc.bsns_type_id, extract(year from act.tran_date) order by extract(month from act.tran_date)),0) as sum_of_transactions_in_last_month	
				--select dc.bsns_type_id,*
	from 	dbo.dim_customer dc
	inner join dbo.acct_tran act
			on dc.bp_main_id =act.bsns_prtn_id
	where 	amnt_type_code='00000'
			and sub_oprn_type='00001'
			and dc.bsns_type_id in (select code from dbo.bsns_acct_type where name in ('Individual','Business'))
	--and dc.bp_main_id =35587
	--and mfloos_spclst_name is not null
	group by dc.bsns_type_id, 1, 2, 3,4,5,6,7,8
	
	) a;
end;
$$
EXECUTE ON ANY;




*Dynamic Query*

CREATE OR REPLACE FUNCTION dbo.get_kjreqeusts(p_fromdate varchar, p_todate varchar, p_mainzone varchar DEFAULT NULL::character varying, p_subzone varchar DEFAULT NULL::character varying, p_branch varchar DEFAULT NULL::character varying, customerid varchar DEFAULT NULL::character varying)
   RETURNS TABLE (company_name_en bpchar, company_code bpchar, co_code varchar, dept_code varchar, customer_id varchar, shortname bpchar, parent bpchar, branchname bpchar, mainzone bpchar, branch2 bpchar, account_number varchar, status varchar, date_time date, authorizar text, inputter_name text, statusreason varchar, date_inp date)
   LANGUAGE plpgsql
   VOLATILE
AS $$
  
  
  
   --select * from dbo.get_kjreqeusts('2022-03-02', '2022-03-04','','','','');
declare
	v_sql_string text;
begin
 
create temp table temp_result as
select * from (
	select
   	co.company_name_en,
   	co.company_code,
   	ivr.co_code,
   	ivr.dept_code,
   	ivr.customer_id::varchar,
   	customer.short_name as shortname,
   	d.dept_parent as parent,
   	d.name as branchname,
   	co.company_name_ar as mainzone,
   	d.dept_acct_off_code as branch2,
   	acct.account_number,
   	ivr.status,
   	date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd'))::date as date_time,
   	f_inputter_name(ivr.authoriser) as authorizar,
   	f_inputter_name(kjm1.inputter) as inputter_name,
   	status_reason,
   	date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd'))::date as date_inp
	from dbo.f_st_kb_ivr_activate ivr
	inner join dbo.fkmb_customer_m1 customer on customer.customer_code::numeric = ivr.customer_id and customer.mv_seq = 2
	left join dbo.f_dept_acct_officer d on d.dept_acct_off_code = ivr.dept_code
	left join dbo.company co on co.company_code = ivr.co_code
	left join dbo.fkmb_account acct on acct.customer = ivr.customer_id
	inner join dbo.f_st_kb_ivr_activate_m1 kjm1 on kjm1.record_id = ivr.record_id and kjm1.mv_seq = 1 and kjm1.sv_seq = 1
	where date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd')) between p_fromdate::date and p_todate::date
	and customer.short_name is not null
	union all
	select
   	co.company_name_en,
   	co.company_code,
   	ivr.co_code,
   	ivr.dept_code,
   	ivr.customer_id::varchar,
   	max(case when customer.mv_seq = 1 then short_name end) as shortname,
   	d.dept_parent as parent,
   	d.name as branchname,
   	co.company_name_ar as mainzone,
   	d.dept_acct_off_code as branch2,
   	acct.account_number,
   	ivr.status,
   	date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd'))::date as date_time,
   	f_inputter_name(ivr.authoriser) as authorizar,
   	f_inputter_name(kjm1.inputter) as inputter_name,
   	status_reason,
   	date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd'))::date as date_inp
	from dbo.f_st_kb_ivr_activate ivr
	inner join dbo.fkmb_customer_m1 customer on customer.customer_code::numeric = ivr.customer_id
	left join dbo.f_dept_acct_officer d on d.dept_acct_off_code = ivr.dept_code
	left join dbo.company co on co.company_code = ivr.co_code
	left join dbo.fkmb_account acct on acct.customer = ivr.customer_id
	inner join dbo.f_st_kb_ivr_activate_m1 kjm1 on kjm1.record_id = ivr.record_id and kjm1.mv_seq = 1 and kjm1.sv_seq = 1
	where date_trunc('day', to_timestamp(substring(kjm1.date_time, 1, 6), 'yymmdd')) between p_fromdate::date and p_todate::date
	and customer.short_name is not null
	group by
   	co.company_name_en,
   	co.company_code,
   	ivr.co_code,
   	ivr.dept_code,
   	ivr.customer_id,
   	d.name,
   	co.company_name_ar,
   	d.dept_acct_off_code,
   	ivr.dept_code,
   	acct.account_number,
   	ivr.status,
   	kjm1.date_time,
   	f_inputter_name(ivr.authoriser),
   	f_inputter_name(kjm1.inputter),
   	status_reason,
   	d.dept_parent
	having max(case when customer.mv_seq = 2 then short_name end) is null
) as temp;
  
   v_sql_string := 'select * from temp_result where 1 = 1 ';
  
   -- append conditions based on input parameters
   if p_mainzone is not null and p_mainzone <> '' then
   	v_sql_string := v_sql_string || ' and co_code = ''' || p_mainzone || '''';
   end if;
  
   if p_subzone is not null and p_subzone <> '' then
   	v_sql_string := v_sql_string || ' and parent in (select * from temp_subzone)';
   end if;
  
   if p_branch is not null and p_branch <> '' then
   	v_sql_string := v_sql_string || ' and dept_code in (select * from temp_branch)';
   end if;
  
   if customer_id is not null and customer_id <> '' then
   	v_sql_string := v_sql_string || ' and customer_id = ''' || customer_id || '''';
   end if;
  
   -- print the constructed sql query
   raise notice '%', v_sql_string;
	return query execute v_sql_string;
 	
   -- drop the temporary table
   drop table if exists temp_result;
end;
$$
EXECUTE ON ANY;



*Query 2 of Dynamic*


CREATE OR REPLACE FUNCTION dbo.get_kj_operation(p_fromdate varchar, p_todate varchar, p_mainzone varchar DEFAULT NULL::character varying, p_subzone varchar DEFAULT NULL::character varying, p_branch varchar DEFAULT NULL::character varying, transaction_type varchar DEFAULT NULL::character varying, p_type_currency varchar DEFAULT NULL::character varying)
	RETURNS TABLE (chanal bpchar, description bpchar, channal varchar, receiver_zone bpchar, receiver_branch bpchar, liquidity_fee bpchar, equvelent_amount bpchar, debit_value_date date, transactiontype bpchar, ref_no bpchar, debit_acct_no bpchar, credit_acct_no bpchar, debit_customer bpchar, debit_currency bpchar, debit_amount text, credit_amount text, credit_customer bpchar, credit_currency bpchar, loc_amt_debited bpchar, loc_amt_credited bpchar, total_charge_amt bpchar, local_charge_amt bpchar, debit_cus_name varchar, cred_cus_name varchar, main_zone bpchar, dept_code bpchar, branch bpchar, sub_zone bpchar, l_narrative bpchar, company_name_en bpchar)
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
--SELECT * from dbo.GET_KJ_Operation('2022-03-02', '2022-03-03', 'YE0012003', '', '', '', 'YER');
BEGIN
   RETURN QUERY
   EXECUTE format(
       'SELECT f.DEBIT_THEIR_REF AS chanal,
               fkm.DESCRIPTION,
               F_chanal(i.INPUTTER) AS channal,
               CODA.COMPANY_NAME_EN AS receiver_zone,
               DD.NAME AS receiver_branch,
               TOT_REC_COMM AS liquidity_fee,
               LOC_AMT_DEBITED AS equvelent_amount,
               F.DEBIT_VALUE_DATE::date,
               F.TRANSACTION_TYPE,
               F.REF_NO,
               F.DEBIT_ACCT_NO,
               F.CREDIT_ACCT_NO,
               F.DEBIT_CUSTOMER,
               F.DEBIT_CURRENCY,
               RIGHT(F.AMOUNT_DEBITED, length(F.AMOUNT_DEBITED)-3) AS debit_amount,
               RIGHT(F.AMOUNT_CREDITED, length(F.AMOUNT_CREDITED)-3) AS credit_amount,
               F.CREDIT_CUSTOMER,
               F.CREDIT_CURRENCY,
               F.LOC_AMT_DEBITED,
               F.LOC_AMT_CREDITED,
               F.TOTAL_CHARGE_AMT,
               F.TOT_REC_CHG AS local_charge_amt,
               F_Get_Cust_Name(F.DEBIT_CUSTOMER, ''A'') AS debit_cus_name,
               F_Get_Cust_Name(F.CREDIT_CUSTOMER, ''A'') AS cred_cus_name,
               CO.COMPANY_NAME_EN AS main_zone,
               F.DEPT_CODE,
               B.NAME AS branch,
               C.name AS sub_zone,
               i.L_NARRATIVE,
               COCR.COMPANY_NAME_EN AS company_name_en
       FROM dbo.FKMB_FUNDS_TRANSFER$HIS AS f
       INNER JOIN dbo.FKMB_FUNDS_TRANSFER$HIS_M1 i ON (i.REF_NO = f.REF_NO AND i.mv_seq=1)
       LEFT JOIN dbo.CUSTOMER ON f.CREDIT_CUSTOMER::numeric = CUSTOMER.CUSTOMER_CODE
       LEFT JOIN dbo.FKMB_ACCOUNT depC ON depC.ACCOUNT_NUMBER = f.CREDIT_ACCT_NO
       LEFT JOIN dbo.F_DEPT_ACCT_OFFICER B ON B.DEPT_ACCT_OFF_CODE = depC.ACCOUNT_OFFICER
       LEFT JOIN dbo.F_DEPT_ACCT_OFFICER C ON C.DEPT_ACCT_OFF_CODE = B.DEPT_PARENT
       LEFT JOIN dbo.COMPANY co ON co.COMPANY_CODE = B.PARENT_COMPANY
       LEFT JOIN dbo.FKMB_ACCOUNT CRAC ON CRAC.ACCOUNT_NUMBER = f.DEBIT_ACCT_NO
       LEFT JOIN dbo.COMPANY CODA ON CODA.COMPANY_CODE = CRAC.CO_CODE
       LEFT JOIN dbo.F_DEPT_ACCT_OFFICER DD ON DD.DEPT_ACCT_OFF_CODE = CRAC.ACCOUNT_OFFICER
       LEFT JOIN dbo.COMPANY COCR ON COCR.COMPANY_CODE = f.CO_CODE
       LEFT JOIN dbo.FKMB_FT_TXN_TYPE_CONDITION_M1 fkm ON f.TRANSACTION_TYPE = fkm.TRANSACTION_TYPE
       AND FKM.mv_seq = 1 AND FKM.sv_seq = 1
       WHERE f.DEPT_CODE IS NOT NULL AND
           F_chanal(i.INPUTTER) <> ''BROWSERTC'' AND
           f.TRANSACTION_TYPE IN (''ACA1'', ''ACA2'', ''ACA3'', ''ACA4'', ''ACA5'', ''ACA6'', ''ACA7'', ''ACA8'', ''ACA9'', ''AC10'', ''ACRP'')
           AND
           F.DEBIT_VALUE_DATE BETWEEN %L AND %L'
           || CASE WHEN P_MAINZONE IS NOT NULL AND P_MAINZONE <> '' THEN
               ' AND f.CO_CODE = ' || quote_literal(P_MAINZONE)
           ELSE '' END
           || CASE WHEN P_SUBZONE IS NOT NULL AND P_SUBZONE <> '' THEN
               ' AND B.DEPT_PARENT IN (SELECT * FROM string_to_table(%L, '',''))'
           ELSE '' END
           || CASE WHEN P_BRANCH IS NOT NULL AND P_BRANCH <> '' THEN
               ' AND B.DEPT_ACCT_OFF_CODE IN (SELECT * FROM string_to_table(%L, '',''))'
           ELSE '' END
           || CASE WHEN TRANSACTION_TYPE IS NOT NULL AND TRANSACTION_TYPE <> '' THEN
               ' AND f.TRANSACTION_TYPE = ' || quote_literal(TRANSACTION_TYPE)
           ELSE '' END
           || CASE WHEN P_TYPE_CURRENCY IS NOT NULL AND P_TYPE_CURRENCY <> '' THEN
               ' AND f.DEBIT_CURRENCY = ' || quote_literal(P_TYPE_CURRENCY)
           ELSE '' END
           || ' ORDER BY dept_code, CO.COMPANY_NAME_EN DESC',
       P_FROMDATE, P_TODATE, P_SUBZONE, P_BRANCH
   );
end;
$$
EXECUTE ON ANY;







*DYNAMIC QUERY EXAMPLE*

Example:
Suppose you have a table named employees with the following columns: employee_id, first_name, last_name, department, and salary. You want to write a generic query that retrieves data from this table based on user-defined criteria, such as filtering by department and sorting by salary.


-- Declare input parameters (can vary based on user input)
DECLARE @department_name VARCHAR(50) = 'Sales';
DECLARE @sort_order VARCHAR(10) = 'DESC';
-- Construct the generic query
SELECT * FROM employees
WHERE
   ( @department_name IS NULL OR department = @department_name )
ORDER BY
   CASE
       WHEN @sort_order = 'ASC' THEN salary
       WHEN @sort_order = 'DESC' THEN salary * -1
       ELSE salary
   END;


Explanation:
We start by declaring input parameters. In this example, we have @department_name and @sort_order as parameters, which can vary based on user input. @department_name is used to filter employees by department, and @sort_order is used to specify the sorting order (ascending or descending).
In the SQL query, we use a SELECT statement to retrieve data from the employees table.
The WHERE clause allows us to filter the results based on the value of @department_name. If @department_name is provided (not NULL), it filters the results to only include employees in the specified department. If @department_name is NULL, it retrieves all employees without applying the department filter.
The ORDER BY clause allows us to sort the results based on the value of @sort_order. We use a CASE expression to determine the sorting order dynamically. If @sort_order is 'ASC', it sorts the results in ascending order of salary. If @sort_order is 'DESC', it sorts the results in descending order of salary. If @sort_order has any other value, it defaults to sorting in ascending order.

