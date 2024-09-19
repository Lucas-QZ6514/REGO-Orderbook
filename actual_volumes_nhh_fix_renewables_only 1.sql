WITH settlement_month AS 
(
    -- This queries the settlement_consumption_monthly table to use as a primary source.
    SELECT
        feccm.contract_key AS "Contract Key",
        feccm.customer_key AS "Customer Key",
        feccm.product_key AS "Product Key",
        fevscm.meter_key AS "Meter Key",
        (
            feccm.quote_mpan || 
            fevscm."year" || 
            LPAD(fevscm."month", 2, '0') || 
            '01' || feccm.contract_key
        ) 
        AS "Generation Key",
        feccm.contract_reference AS "Quote ID",
        fevscm.mpan AS "MPAN", 
        fevscm.meter_type AS "Meter Type",
        fevscm."month" AS "Month", 
        fevscm."year" AS "Year",
        TO_TIMESTAMP(fevscm."year" || '-' || LPAD(fevscm."month", 2, '0') || '-01', 'YYYY-MM-DD') AS "Delivery Month",
        fevscm.settlement_site_volume_kwh / 1000 AS "Actual MSP (MWh)",
        fevscm.settlement_gsp_volume_kwh / 1000 AS "Actual GSP (MWh)",
        fevscm.settlement_nbp_volume_kwh / 1000 AS "Actual NBP (MWh)"
    FROM 
        edw_elec_volumes_cdh_views.fact_elec_volumes_settlement_consumption_monthly fevscm 
    JOIN    
        edw_cdh_views.fact_elec_consumption_current_monthly feccm ON fevscm.meter_key = feccm.meter_key
    JOIN
        edw_cdh_views.dim_customer dc ON feccm.customer_key = dc.customer_key
    WHERE
        (
            (fevscm.meter_type = 'HH' AND fevscm.measurement_quantity_id = 'AI' AND fevscm.actual_estimate_indicator = 'A')
            OR (fevscm.meter_type = 'NHH' AND fevscm.measurement_quantity_id IS NULL AND fevscm.actual_estimate_indicator = 'A')
        )
        AND (
            (fevscm."year" = '2023' AND fevscm."month" >= '4') 
            OR (fevscm."year" = '2024' AND fevscm."month" <= '3')
        )
    GROUP BY 
        feccm.contract_key,
        feccm.customer_key,
        feccm.product_key,
        fevscm.meter_key,
        feccm.contract_reference,
        dc.companyno,
        feccm.quote_mpan,
        fevscm.mpan,
        fevscm.meter_type,
        fevscm."month",
        fevscm."year",
        fevscm.settlement_site_volume_kwh,
        fevscm.settlement_gsp_volume_kwh,
        fevscm.settlement_nbp_volume_kwh,
        fevscm.measurement_quantity_id,
        fevscm.actual_estimate_indicator
    ORDER BY 
        fevscm."year" ASC, 
        fevscm."month" ASC,
        feccm.contract_key ASC
),
agreement_dates AS (
    -- This queries the contract table so we can use the agreement start and end to ensure the right dates come through.
    SELECT
        dim.contract_key AS "Contract Key",
        dim.agreement_number AS "Agreement Number",
        dim.agreement_start_date AS "Agreement Start Date",
        dim.agreement_end_date AS "Agreement End Date",
        dim.contract_reference AS "Quote ID",
        dim.quote_model AS "Quote Model",
        ROW_NUMBER() OVER (
            PARTITION BY dim.contract_reference || fact.MPAN
            ORDER BY dim.agreement_number DESC
        ) AS RowNum
    FROM
        edw_cdh_views.dim_elec_contract dim
    JOIN
        edw_cdh_views.fact_elec_consumption_current_monthly fact
        ON dim.contract_key = fact.contract_key
    WHERE
        (
            (dim.agreement_start_date >= '2023-04-01 00:00:00' AND dim.agreement_start_date <= '2024-03-31 23:59:59') OR
            (dim.agreement_end_date >= '2023-04-01 00:00:00' AND dim.agreement_end_date <= '2024-03-31 23:59:59') OR
            (dim.agreement_start_date <= '2023-04-01 00:00:00' AND dim.agreement_end_date >= '2024-03-31 23:59:59')
        )
)
SELECT
    settlement_month."Contract Key",
    settlement_month."Customer Key",
    settlement_month."Generation Key",
    settlement_month."Quote ID",
    settlement_month."MPAN",
    settlement_month."Meter Type",
    settlement_month."Delivery Month",
    settlement_month."Actual MSP (MWh)",
    settlement_month."Actual GSP (MWh)",
    settlement_month."Actual NBP (MWh)"
FROM
    settlement_month
JOIN
    agreement_dates ad ON ad."Contract Key" = settlement_month."Contract Key"
WHERE
    ad.RowNum = 1
    AND settlement_month."Delivery Month" BETWEEN ad."Agreement Start Date" AND ad."Agreement End Date"
    AND (
        (ad."Quote Model" LIKE '%Renewable%' AND ad."Quote Model" NOT LIKE '%No Renewable%') 
        OR ad."Quote ID" IN (
            'Q00286479', 'Q00366719', 'Q00346654', 'Q00301397', 'Q00301429', 'Q00680143',
            'Q00698018', 'Q00307361', 'Q00413840', 'Q00335080', 'Q00342497',
            'Q00440103', 'Q00335033', 'Q00367887', 'Q00335027', 'Q00335071', 'Q00410605',
            'Q00335100', 'Q00352684', 'Q00335041', 'Q00440104', 'Q00367332', 'Q00778757',
            'Q00270944', 'Q00275862', 'Q00305046'
        )
    )
GROUP BY
    settlement_month."Contract Key",
    settlement_month."Customer Key",
    settlement_month."Generation Key",
    settlement_month."Quote ID",
    settlement_month."MPAN",
    settlement_month."Meter Type",
    settlement_month."Delivery Month",
    settlement_month."Actual MSP (MWh)",
    settlement_month."Actual GSP (MWh)",
    settlement_month."Actual NBP (MWh)"
ORDER BY
    settlement_month."Contract Key" ASC,
    settlement_month."Generation Key" ASC,
    settlement_month."Delivery Month" ASC