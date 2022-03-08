CREATE OR REPLACE TABLE `analytics-supplychain-thd.BXV8EDM.PROMOTE_ZIP_IDENTIFICATION` AS

SELECT
  B.*,
  C.DFC_ORDER_QTY,
  B.SALES_HISTORY/C.DFC_ORDER_QTY AS ZIP_PCNT_DFC_HISTORY ## % that will be leaving the CURRENT DFC FOR promoted dfc
FROM (
  SELECT
    ZIP_CODE,
    SKU_NBR,
    CURRENT_DFC,
    CONCAT(CURRENT_DFC, " - ", CURRENT_DFC_NAME) AS CURRENT_DFC_LOC,
    CURRENT_RANK,
    PROMOTE_DFC,
    CONCAT(PROMOTE_DFC," - ",PROMOTE_DFC_NAME) AS PROMOTE_DFC_LOC,
    PROMOTE_DFC_RANK,
    CASE
      WHEN CURRENT_RANK > PROMOTE_DFC_RANK THEN CONCAT(PROMOTE_DFC," - ",PROMOTE_DFC_NAME)
    ELSE
    CONCAT(CURRENT_DFC, " - ", CURRENT_DFC_NAME)
  END
    AS GO_FWD_DFC,
    ACTION,
    QTY AS SALES_HISTORY
  FROM (
    SELECT
      A.*,
      CASE
        WHEN SUM(SALES.ORDERED_QTY) IS NULL THEN 0
      ELSE
      SUM(SALES.ORDERED_QTY)
    END
      AS QTY,
      ROW_NUMBER() OVER (PARTITION BY A.SKU_NBR, A.ZIP_CODE ORDER BY A.CURRENT_RANK ) AS ROWNUM
    FROM (
      SELECT
        DISTINCT SKU.SKU_NBR,
        sku.LOC_NBR,
        ZIP.ZIP_CODE,
        ZIP.DFC AS CURRENT_DFC,
        ZIP.CAMPUS_NM AS CURRENT_DFC_NAME,
        ZIP.DFC_POS AS CURRENT_RANK,
        PRMT.DFC AS PROMOTE_DFC,
        PRMT.CAMPUS_NM AS PROMOTE_DFC_NAME,
        PRMT.DFC_POS AS PROMOTE_DFC_RANK,
        CASE
          WHEN PRMT.DFC_POS < ZIP.DFC_POS THEN 'PROMOTE'
        ELSE
        'NONE'
      END
        AS ACTION
      FROM
        `pr-edw-views-thd.SCHN_FCST.OUTPUT_HISTORY` SKU
      LEFT JOIN
        `analytics-df-thd.DELV_ANALYTICS.DFC_ROUTING_HIST_XFRM_FUTURE_STATE` ZIP
      ON
        CAST (ZIP.DFC AS INT64) =sku.LOC_NBR
        AND ZIP.IS_ACTIVE IS TRUE
      LEFT JOIN
        `analytics-df-thd.DELV_ANALYTICS.DFC_ROUTING_HIST_XFRM_FUTURE_STATE` PRMT
      ON
        ZIP.ZIP_CODE = PRMT.ZIP_CODE
        AND PRMT.IS_ACTIVE IS TRUE
        AND PRMT.Zip_Code IS NOT NULL
      WHERE
        1 = 1
        AND SKU_NBR IN (1002676874)
        AND REPLENISHMENT_DATE = CURRENT_DATE
        AND CAL_DT = CURRENT_DATE
        AND ZIP.ZIP_CODE IS NOT NULL
        AND PRMT.DFC = '5854' ) A
    LEFT JOIN
      `pr-edw-views-thd.ORD_COM.COM_LINE` SALES
    ON
      A.SKU_Nbr = SALES.SKU_NBR
      AND A.ZIP_CODE = SALES.ZIP_CODE
      AND CAST(ORDER_DATE AS date) >= (CURRENT_DATE() -186)
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10)
  WHERE
    1=1
    AND ROWNUM = 1) B
LEFT JOIN (
  SELECT
    A.SKU_NBR,
    A.LOC_NBR,
    CONCAT(CURRENT_DFC," - ", CURRENT_DFC_NAME) AS DFC_CURRENT,
    SUM(SALES.ORDERED_QTY) AS DFC_ORDER_QTY
  FROM (
    SELECT
      DISTINCT SKU.SKU_NBR,
      sku.LOC_NBR,
      ZIP.ZIP_CODE,
      ZIP.DFC AS CURRENT_DFC,
      ZIP.CAMPUS_NM AS CURRENT_DFC_NAME,
      ZIP.DFC_POS AS CURRENT_RANK,
      ROW_NUMBER() OVER (PARTITION BY sku.SKU_NBR, zip.ZIP_CODE ORDER BY ZIP.DFC_POS ) AS ROWNUM
    FROM
      `pr-edw-views-thd.SCHN_FCST.OUTPUT_HISTORY` SKU
    LEFT JOIN
      `analytics-df-thd.DELV_ANALYTICS.DFC_ROUTING_HIST_XFRM_FUTURE_STATE` ZIP
    ON
      CAST (ZIP.DFC AS INT64) =sku.LOC_NBR
      AND ZIP.IS_ACTIVE IS TRUE
    WHERE
      1 = 1
      AND SKU_NBR IN (1002676874)
      AND REPLENISHMENT_DATE = CURRENT_DATE
      AND CAL_DT = CURRENT_DATE
      AND ZIP.ZIP_CODE IS NOT NULL) A
  LEFT JOIN
    `pr-edw-views-thd.ORD_COM.COM_LINE` SALES
  ON
    A.SKU_Nbr = SALES.SKU_NBR
    AND A.ZIP_CODE = SALES.ZIP_CODE
    AND CAST(ORDER_DATE AS date) >= (CURRENT_DATE() -186)
    AND SALES.ZIP_CODE IS NOT NULL
  WHERE
    1=1
    AND ROWNUM = 1
  GROUP BY
    1,
    2,
    3 ) C
ON
  B.SKU_NBR = C.SKU_NBR
  AND B.CURRENT_DFC_LOC = C.DFC_CURRENT
WHERE
  1=1
  --AND ACTION = 'PROMOTE'
  AND B.SALES_HISTORY >= 0
ORDER BY
  ZIP_CODE;

------

CREATE OR REPLACE TABLE `analytics-supplychain-thd.BXV8EDM.PROMOTE_NEW_MAX` AS

SELECT
  SKU_NBR,
  CURRENT_DFC_NAME,
  CURRENT_DFC_LOC,
  PROMOTE_DFC_LOC,
  ON_HAND,
  PACK_SIZE,
  CYCL_STK,
  MIN_OH,
  ADS_FORECAST,
  SAFETY_STOCK,
  COMMITTED_STOCK,
  MAX_OH,
  OUTL,
  PCNT_SALE_PROMOTE,
  CYCLE_STOCK_NEW,
  LEAST((CYCLE_STOCK_NEW+GREATEST (MIN_OH,ADS_FORECAST,SAFETY_STOCK)+COMMITTED_STOCK),MAX_OH) AS NEW_OUTL
FROM (
  SELECT
    D.*,
    E.DFC_ORDER_QTY,
    E.CURRENT_DFC_LOC,
    E.PROMOTE_DFC_LOC,
    CASE
      WHEN (SUM(E.SALES_HISTORY)/E.DFC_ORDER_QTY) IS NULL THEN 0
    ELSE
    SUM(E.SALES_HISTORY)/E.DFC_ORDER_QTY
  END
    AS PCNT_SALE_PROMOTE,
    CASE
      WHEN D.CYCL_STK*(SUM(E.SALES_HISTORY)/E.DFC_ORDER_QTY) IS NULL THEN D.CYCL_STK
    ELSE
    D.CYCL_STK*(1-(SUM(E.SALES_HISTORY)/E.DFC_ORDER_QTY))
  END
    AS CYCLE_STOCK_NEW,
  FROM (
    SELECT
      DISTINCT SKU.SKU_NBR,
      DFC.DC_NBR AS CURRENT_DFC,
      DFC.DC_NM AS CURRENT_DFC_NAME,
      SKU.ON_HAND,
      SKU.CYCL_STK,
      SKU.MIN_OH,
      SKU.ADS_FORECAST,
      SKU.SAFETY_STOCK,
      SKU.COMMITTED_STOCK,
      SKU.MAX_OH,
      SKU.OUTL,
      SKU.PACK_SIZE
    FROM
      `pr-edw-views-thd.SCHN_FCST.OUTPUT_HISTORY` SKU
    LEFT JOIN
      `pr-edw-views-thd.SHARED.DC_HIER_FD` DFC
    ON
      CAST (DFC.DC_NBR AS INT64) = SKU.LOC_NBR
    WHERE
      1 = 1
      AND SKU_NBR IN (1002676874)
      AND REPLENISHMENT_DATE = CURRENT_DATE
      AND CAL_DT = CURRENT_DATE
      AND DFC.DC_TYP_CD = 'DFC') D
  LEFT JOIN
    `analytics-supplychain-thd.BXV8EDM.PROMOTE_ZIP_IDENTIFICATION` E
  ON
    E.SKU_NBR = D.SKU_NBR
    AND E.CURRENT_DFC = D.CURRENT_DFC
  WHERE
  1=1
  AND E.ACTION = 'PROMOTE'
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15
   );

------
    select *, ROW_NUMBER() over (partition by SKU_NBR order by AVAILABLE_TO_TRANSFER_FLOOR desc) Ranking
    from (
             SELECT SKU_NBR,
                    CURRENT_DFC_NAME,
                    NEW_OUTL,
                    ON_HAND,
                    PACK_SIZE,
                    GREATEST(0, ON_HAND - (NEW_OUTL + PACK_SIZE))                                AS AVAILABLE_TO_TRANSFER,
                    GREATEST(0, ON_HAND - (NEW_OUTL + PACK_SIZE)) / PACK_SIZE                    AS AVAILABLE_TO_TRANSFER_ROUNDED,
                    FLOOR(GREATEST(0, ON_HAND - (NEW_OUTL + PACK_SIZE)) / PACK_SIZE)             AS AVAILABLE_TO_TRANSFER_FLOOR_RAW,
                    FLOOR(GREATEST(0, ON_HAND - (NEW_OUTL + PACK_SIZE)) / PACK_SIZE) *
                    PACK_SIZE                                                                    AS AVAILABLE_TO_TRANSFER_FLOOR, -- Solves rounding down
             FROM `analytics-supplychain-thd.BXV8EDM.PROMOTE_NEW_MAX`
         );

------

    SELECT
           SKU_NBR,
           PROMOTE_DFC_LOC,
           round((SUM(SALES_HISTORY) / 26) * 4) AS WOS_TRANSFER_TARGET
    FROM `analytics-supplychain-thd.BXV8EDM.PROMOTE_ZIP_IDENTIFICATION`
    WHERE 1 = 1
      AND ACTION = 'PROMOTE'
    GROUP BY 1,
             2
    order by WOS_TRANSFER_TARGET desc;


---------------------------------------------------------------------------

with avail_transfer as -- pooled lots
    (
    select *, ROW_NUMBER() over (partition by SKU_NBR order by AVAILABLE_TO_TRANSFER_FLOOR desc) Ranking
    from (
             SELECT SKU_NBR,
                    CURRENT_DFC_NAME,
                    CURRENT_DFC_LOC,
                    NEW_OUTL,
                    ON_HAND,
                    PACK_SIZE,
                    FLOOR(GREATEST(0, ON_HAND - (NEW_OUTL + PACK_SIZE)) / PACK_SIZE) *
                    PACK_SIZE                                                                    AS AVAILABLE_TO_TRANSFER_FLOOR, -- Solves rounding down
             FROM `analytics-supplychain-thd.BXV8EDM.PROMOTE_NEW_MAX`
    )),
     -- max of 1 buy back

promote_target as ( -- quanitity comsumed
------
    SELECT SKU_NBR,
           PROMOTE_DFC_LOC,
           round((SUM(SALES_HISTORY) / 26) * 4) AS WOS_TRANSFER_TARGET
    FROM `analytics-supplychain-thd.BXV8EDM.PROMOTE_ZIP_IDENTIFICATION`
    WHERE 1 = 1
      AND ACTION = 'PROMOTE'
    GROUP BY 1,
             2
    order by WOS_TRANSFER_TARGET desc
)
-- ,base as (
    select tar.SKU_NBR,
           CURRENT_DFC_LOC,
           PROMOTE_DFC_LOC,
           AVAILABLE_TO_TRANSFER_FLOOR,
           WOS_TRANSFER_TARGET,
           CASE

               when AVAILABLE_TO_TRANSFER_FLOOR is null then WOS_TRANSFER_TARGET
               when AVAILABLE_TO_TRANSFER_FLOOR >= WOS_TRANSFER_TARGET
                   then WOS_TRANSFER_TARGET - AVAILABLE_TO_TRANSFER_FLOOR
               when AVAILABLE_TO_TRANSFER_FLOOR < WOS_TRANSFER_TARGET then 0
               END Running_Quanitity,

           CASE
               when AVAILABLE_TO_TRANSFER_FLOOR is null then 0
               when AVAILABLE_TO_TRANSFER_FLOOR >= WOS_TRANSFER_TARGET then 0
               when AVAILABLE_TO_TRANSFER_FLOOR < WOS_TRANSFER_TARGET
                   then WOS_TRANSFER_TARGET - AVAILABLE_TO_TRANSFER_FLOOR
               END Remaining_Quantity,
    from avail_transfer tran
             left join promote_target tar
                       on tar.SKU_NBR = tran.SKU_NBR
    order by AVAILABLE_TO_TRANSFER_FLOOR desc
)
                select
                       base.SKU_NBR,
                       CURRENT_DFC_LOC,
                       base.PROMOTE_DFC_LOC,
                       AVAILABLE_TO_TRANSFER_FLOOR,
                       base.WOS_TRANSFER_TARGET,
                       case
                        when Running_Quanitity + AVAILABLE_TO_TRANSFER_FLOOR >= Remaining_Quantity then Running_Quanitity + AVAILABLE_TO_TRANSFER_FLOOR - Remaining_Quantity
                        when Running_Quanitity + AVAILABLE_TO_TRANSFER_FLOOR < Remaining_Quantity then 0
                        end Running_QTY,
                      case
                        when Running_Quanitity + AVAILABLE_TO_TRANSFER_FLOOR >= Remaining_Quantity then 0
                        when Running_Quanitity + AVAILABLE_TO_TRANSFER_FLOOR < Remaining_Quantity then Remaining_Quantity - Running_Quanitity - AVAILABLE_TO_TRANSFER_FLOOR
                      end Remaining_QTY
                from base
                    inner join
                    promote_target tar on base.SKU_NBR = tar.SKU_NBR

