WITH forecast_volumes AS (
    SELECT
        feccm.contract_key AS "Contract Key",
        feccm.customer_key AS "Customer Key",
        feccm.product_key AS "Product Key",
        feccm.meter_key AS "Meter Key",
        (
            feccm.quote_mpan || 
            feccm."year" || 
            LPAD(feccm."month", 2, '0') || 
            '01' || feccm.contract_key
        ) AS "Generation Key",
        feccm.contract_reference AS "Quote ID",
        feccm.mpan AS "MPAN",
        feccm."year",
        feccm."month",
        TO_TIMESTAMP(feccm."year" || '-' || LPAD(feccm."month", 2, '0') || '-01', 'YYYY-MM-DD') AS "Delivery Month",
        SUM(feccm.site_volume_kwh) / 1000 AS "Forecast MSP Volume (MWh)",
        SUM(feccm.gsp_volume_kwh) / 1000 AS "Forecast GSP Volume (MWh)",
        SUM(feccm.nbp_volume_kwh) / 1000 AS "Forecast NBP Volume (MWh)",
        SUM(feccm.reforecasted_msp_volume_st) / 1000 AS "Forecast ST MSP Volume (MWh)",
        SUM(feccm.reforecasted_gsp_volume_st) / 1000 AS "Forecast ST GSP Volume (MWh)",
        SUM(feccm.reforecasted_nbp_volume_st) / 1000 AS "Forecast ST NBP Volume (MWh)",
        SUM(feccm.reforecasted_msp_volume_lt) / 1000 AS "Forecast LT MSP Volume (MWh)",
        SUM(feccm.reforecasted_gsp_volume_lt) / 1000 AS "Forecast LT GSP Volume (MWh)",
        SUM(feccm.reforecasted_nbp_volume_lt) / 1000 AS "Forecast LT NBP Volume (MWh)",
        feccm.quote_mpan ||
        CASE
            WHEN dc.companyno ~ '^[0-9]' THEN
                CASE
                    WHEN LENGTH(dc.companyno) < 8 THEN LPAD(dc.companyno, 8, '0')
                    ELSE dc.companyno
                END
            ELSE COALESCE(dc.companyno, '')
        END AS Meter_UID
    FROM
        edw_cdh_views.fact_elec_consumption_current_monthly feccm
    JOIN
        edw_cdh_views.dim_customer dc ON feccm.customer_key = dc.customer_key
    WHERE 
        (("year" = '2023' AND "month" >= '4') OR ("year" = '2024' AND "month" <= '3'))
    GROUP BY 
        feccm.contract_key,
        feccm.customer_key,
        feccm.product_key,
        feccm.meter_key,
        dc.companyno,
        feccm.mpan,
        feccm."month",
        feccm."year",
        feccm.contract_reference,
        feccm.quote_mpan
    ORDER BY 
        feccm."year" ASC, 
        feccm."month" ASC
),
agreement_dates AS (
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
        edw_cdh_views.fact_elec_consumption_current_monthly fact ON dim.contract_key = fact.contract_key
    WHERE
        (
            (dim.agreement_start_date >= '2023-04-01 00:00:00' AND dim.agreement_start_date <= '2024-03-31 23:59:59') OR
            (dim.agreement_end_date >= '2023-04-01 00:00:00' AND dim.agreement_end_date <= '2024-03-31 23:59:59') OR
            (dim.agreement_start_date <= '2023-04-01 00:00:00' AND dim.agreement_end_date >= '2024-03-31 23:59:59')
        )
)
SELECT
    forecast_volumes."Contract Key",
    forecast_volumes."Customer Key",
    forecast_volumes."Product Key",
    forecast_volumes."Generation Key",
    forecast_volumes."Quote ID",
    forecast_volumes."MPAN",
    forecast_volumes."Delivery Month",
    forecast_volumes."Forecast MSP Volume (MWh)",
    forecast_volumes."Forecast GSP Volume (MWh)",
    forecast_volumes."Forecast NBP Volume (MWh)",
    forecast_volumes."Forecast ST MSP Volume (MWh)",
    forecast_volumes."Forecast ST GSP Volume (MWh)",
    forecast_volumes."Forecast ST NBP Volume (MWh)",
    forecast_volumes."Forecast LT MSP Volume (MWh)",
    forecast_volumes."Forecast LT GSP Volume (MWh)",
    forecast_volumes."Forecast LT NBP Volume (MWh)"
FROM 
    forecast_volumes
JOIN
    agreement_dates ad ON ad."Contract Key" = forecast_volumes."Contract Key"
WHERE
    ad.RowNum = 1
    AND forecast_volumes."Delivery Month" BETWEEN ad."Agreement Start Date" AND ad."Agreement End Date"
    AND (ad."Quote Model" LIKE '%Renewable%' AND ad."Quote Model" NOT LIKE '%No Renewable%') 
    OR (ad."Quote ID" IN ('Q00286479', 'Q00366719', 'Q00346654', 'Q00301397', 'Q00301429', 'Q00680143',
        'Q00698018', 'Q00307361', 'Q00413840', 'Q00413840', 'Q00335080', 'Q00342497',
        'Q00440103', 'Q00335033', 'Q00367887', 'Q00335027', 'Q00335071', 'Q00410605',
        'Q00335100', 'Q00352684', 'Q00335041', 'Q00440104', 'Q00367332', 'Q00778757',
        'Q00270944', 'Q00275862', 'Q00305046'))
GROUP BY
    forecast_volumes."Contract Key",
    forecast_volumes."Customer Key",
    forecast_volumes."Product Key",
    forecast_volumes."Generation Key",
    forecast_volumes."Quote ID",
    forecast_volumes."MPAN",
    forecast_volumes."Delivery Month",
    forecast_volumes."Forecast MSP Volume (MWh)",
    forecast_volumes."Forecast GSP Volume (MWh)",
    forecast_volumes."Forecast NBP Volume (MWh)",
    forecast_volumes."Forecast ST MSP Volume (MWh)",
    forecast_volumes."Forecast ST GSP Volume (MWh)",
    forecast_volumes."Forecast ST NBP Volume (MWh)",
    forecast_volumes."Forecast LT MSP Volume (MWh)",
    forecast_volumes."Forecast LT GSP Volume (MWh)",
    forecast_volumes."Forecast LT NBP Volume (MWh)"
ORDER BY
    forecast_volumes."Contract Key" ASC,
    forecast_volumes."Generation Key" ASC,
    forecast_volumes."Delivery Month" ASC
