/* 
 * Fact_Elec_Volumes
 * 
 */

WITH contract AS (

    SELECT DISTINCT 		contract_key 
                            , DATE(agreement_start_date) AS agreement_start_date
                            , CASE 
                                WHEN agreement_cancelled_flag = 'Y'
                                THEN DATE(agreement_cancellation_date) 
                                ELSE DATE(agreement_end_date)
                            END AS agreement_end_date 
                            , agreement_cancelled_flag  
                            , agreement_cancellation_date

    FROM 					edw_cdh_views.dim_elec_contract

),

------------------------------------------------------------------------------------------------------------------

quote_mpans AS (
    
    SELECT DISTINCT quote_mpan 
    
    FROM    edw_cdh_views.fact_elec_cost_current_monthly 
    
    WHERE   1 = 1   

        AND     costcomponent_key = '59b1310dc7b649a054a4c396b7181b73'
        AND     (year >= 2023 OR TO_DATE(contract_won_date_key, 'YYYYMMDD') BETWEEN TO_DATE('2021-09-13', 'YYYY-MM-DD') AND TO_DATE('2022-09-21', 'YYYY-MM-DD'))
        AND     quote_mpan NOT IN (${quote_mpans})
    
),
------------------------------------------------------------------------------------------------------------------

ltf AS (

    WITH lf AS (

        SELECT DISTINCT contract_key 
                        , delivery_date_key
                        , CASE 
                            WHEN itron_forecast_key_lt IS NULL
                            THEN 'N'
                            ELSE 'Y'
                        END AS has_ltf

        FROM	 		edw_cdh_views.fact_elec_consumption_current_daily

        WHERE 			1 = 1

            AND 		contract_key IS NOT NULL

        ORDER BY 		1, 2

    ), 

    lf_distinct AS (

        SELECT 		contract_key 
                    , delivery_date_key 
                    , MIN(has_ltf) AS has_ltf

        FROM 		lf 

        GROUP BY 	1,2

    )

    SELECT 		*

    FROM 		lf_distinct 

),

------------------------------------------------------------------------------------------------------------------

consumption AS (

    WITH volumes AS (

        SELECT 			contract_key
                        , contract_reference AS quote
                        , meter_key
                        , customer_key
                        , product_key
                        , TO_DATE(delivery_date_key, 'YYYYMMDD') AS delivery_date
                        , SUBSTRING(delivery_date_key, 1, 6) AS calendarmonthkey 
                        , TO_DATE(contract_won_date_key, 'YYYYMMDD') AS contract_won_date
                        , delivery_date_key
                        , fin_rep_period
                        , snapshot_date
                        , quote_mpan
                        , nbp_volume_kwh
                        , gsp_volume_kwh
                        , site_volume_kwh AS msp_volume_kwh
                        , reforecasted_nbp_volume_lt
                        , reforecasted_gsp_volume_lt
                        , reforecasted_msp_volume_lt

        FROM 			edw_cdh_views.fact_elec_consumption_current_daily

        WHERE 			1 = 1

            AND 		contract_key IS NOT NULL 
            AND 		meter_key IS NOT NULL 
            AND 		customer_key IS NOT NULL
            AND		 	product_key IS NOT NULL
            AND         quote_mpan IN (SELECT * FROM quote_mpans)

    ), 

    distinct_volumes AS (

        SELECT DISTINCT * 

        FROM 	volumes 

    ), 

    ltf_filtered AS (

        SELECT 		sv.contract_key
                    , sv.quote
                    , meter_key
                    , customer_key
                    , product_key
                    , sv.delivery_date
                    , sv.calendarmonthkey
                    , sv.contract_won_date
                    , sv.fin_rep_period
                    , sv.snapshot_date
                    , sv.quote_mpan
                    , COALESCE(nbp_volume_kwh, 0) AS nbp_volume_kwh
                    , COALESCE(gsp_volume_kwh, 0) AS gsp_volume_kwh
                    , COALESCE(msp_volume_kwh, 0) AS msp_volume_kwh
                    , COALESCE(CASE 
                        WHEN ltf.has_ltf = 'Y' THEN reforecasted_nbp_volume_lt
                        ELSE nbp_volume_kwh
                    END, 0) AS reforecasted_nbp_volume_kwh	
                    , COALESCE(CASE 
                        WHEN ltf.has_ltf = 'Y' THEN reforecasted_gsp_volume_lt
                        ELSE gsp_volume_kwh
                    END, 0) AS reforecasted_gsp_volume_kwh	
                    , COALESCE(CASE 
                        WHEN ltf.has_ltf = 'Y' THEN reforecasted_msp_volume_lt
                        ELSE msp_volume_kwh
                    END, 0) AS reforecasted_msp_volume_kwh				

        FROM 		distinct_volumes sv

        LEFT JOIN 	ltf

        ON 			sv.contract_key = ltf.contract_key
            AND 	sv.delivery_date_key = ltf.delivery_date_key

    )

    SELECT 		contract_key
                , quote
                , meter_key
                , customer_key 
                , product_key
                , delivery_date
                , calendarmonthkey
                , contract_won_date
                , fin_rep_period
                , snapshot_date
                , quote_mpan
                , SUM(nbp_volume_kwh) AS nbp_volume_kwh
                , SUM(gsp_volume_kwh) AS gsp_volume_kwh
                , SUM(msp_volume_kwh) AS msp_volume_kwh
                , SUM(reforecasted_nbp_volume_kwh) AS reforecasted_nbp_volume_kwh
                , SUM(reforecasted_gsp_volume_kwh) AS reforecasted_gsp_volume_kwh
                , SUM(reforecasted_msp_volume_kwh) AS reforecasted_msp_volume_kwh


    FROM 	 	ltf_filtered

    GROUP BY 	1,2,3,4,5,6,7,8,9,10,11

),

------------------------------------------------------------------------------------------------------------------

outside_contract AS (

    SELECT 		consumption.contract_key	
                , QUOTE
                , meter_key
                , consumption.customer_key
                , consumption.product_key
                , delivery_date
                , calendarmonthkey
                , contract_won_date
                , fin_rep_period
                , snapshot_date
                , contract.agreement_cancelled_flag
                , contract.agreement_cancellation_date:: date AS agreement_cancellation_date
                , quote_mpan
                , nbp_volume_kwh
                , gsp_volume_kwh
                , msp_volume_kwh
                , CASE 
                    WHEN delivery_date NOT BETWEEN agreement_start_date AND agreement_end_date
                    THEN 0
                    ELSE reforecasted_nbp_volume_kwh
                END AS reforecasted_nbp_volume_kwh
                , CASE 
                    WHEN delivery_date NOT BETWEEN agreement_start_date AND agreement_end_date
                    THEN 0
                    ELSE reforecasted_gsp_volume_kwh
                END AS reforecasted_gsp_volume_kwh
                , CASE 
                    WHEN delivery_date NOT BETWEEN agreement_start_date AND agreement_end_date
                    THEN 0
                    ELSE reforecasted_msp_volume_kwh
                END AS reforecasted_msp_volume_kwh

    FROM 		consumption

    LEFT JOIN 	contract 

    ON 			consumption.contract_key = contract.contract_key

),

------------------------------------------------------------------------------------------------------------------

recent_keys AS (

    SELECT      quote
                , contract_key
                , meter_key
                , customer_key
                , product_key
                , calendarmonthkey
                , contract_won_date
                , fin_rep_period
                , snapshot_date
                , agreement_cancelled_flag
                , agreement_cancellation_date
                , nbp_volume_kwh
                , gsp_volume_kwh
                , msp_volume_kwh
                , reforecasted_nbp_volume_kwh
                , reforecasted_gsp_volume_kwh
                , reforecasted_msp_volume_kwh
                , quote_mpan
                , LAST_VALUE(contract_key) OVER (PARTITION BY quote_mpan
                                                ORDER BY delivery_date
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_contract_key
                , LAST_VALUE(meter_key) OVER (PARTITION BY quote_mpan
                                            ORDER BY delivery_date
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_meter_key
                , LAST_VALUE(customer_key) OVER (PARTITION BY quote_mpan
                                                ORDER BY delivery_date
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_customer_key
                , LAST_VALUE(product_key) OVER (PARTITION BY quote_mpan
                                                ORDER BY delivery_date
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_product_key
                , MAX(agreement_cancelled_flag) OVER (PARTITION BY quote_mpan) AS last_agreement_cancelled_flag 
                , LAST_VALUE(agreement_cancellation_date) IGNORE NULLS OVER (PARTITION BY quote_mpan
                                                                            ORDER BY delivery_date
                                                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agreement_cancellation_date

    FROM        outside_contract 

)

------------------------------------------------------------------------------------------------------------------

SELECT 		QUOTE
			, contract_key
            , meter_key
            , customer_key
            , product_key
            , calendarmonthkey
            , contract_won_date
            , fin_rep_period
            , snapshot_date
            , agreement_cancelled_flag
            , agreement_cancellation_date
            , CAST(SUM(nbp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS quoted_nbp_volume_mwh
            , CAST(SUM(gsp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS quoted_gsp_volume_mwh
            , CAST(SUM(msp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS quoted_msp_volume_mwh
            , CAST(SUM(reforecasted_nbp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS reforecasted_nbp_volume_mwh
            , CAST(SUM(reforecasted_gsp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS reforecasted_gsp_volume_mwh
            , CAST(SUM(reforecasted_msp_volume_kwh):: float / 1000:: float AS DECIMAL(10,2)) AS reforecasted_msp_volume_mwh             
            , quote_mpan
            , last_contract_key 
            , last_meter_key 
            , last_customer_key
            , last_product_key 
            , last_agreement_cancelled_flag
            , last_agreement_cancellation_date

FROM		recent_keys

GROUP BY 	1,2,3,4,5,6,7,8,9,10,11,quote_mpan
            , last_contract_key 
            , last_meter_key 
            , last_customer_key
            , last_product_key 
            , last_agreement_cancelled_flag
            , last_agreement_cancellation_date