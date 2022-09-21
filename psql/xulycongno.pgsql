SELECT
*,
CASE
	WHEN debtincharge = 'MDS' THEN
		CASE
			WHEN daydiff < 0 THEN 'no_xanh'
			WHEN daydiff <= 5 THEN 'no_vang'
			WHEN daydiff >5 and daydiff <=9 then 'no_do'
			ELSE 'no_den'
		END
	WHEN debtincharge = 'CS' THEN
		CASE
			WHEN daydiff-dayterms < 0 THEN 'no_xanh'
			WHEN daydiff-dayterms <= dayterms/2 THEN 'no_vang'
			WHEN daydiff-dayterms > dayterms/2 AND daydiff-dayterms <= dayterms THEN 'no_do'
			ELSE 'no_den'
		END
END AS debttype

FROM
(
SELECT
*,
(CAST(duedate as date) - CAST(dateoforder as date)) as dayterms,

CAST(now() as date) - CAST(duedate as date) as daydiff,

CASE
	WHEN dongiaothanhcong = 1 then 'Don Giao Thanh Cong'
	WHEN donhuy = 1 then 'Don Huy'
	WHEN donchuagiao = 1 then 'Don Chua Giao'
	ELSE ''
END AS typedon,

CASE
	WHEN tiengiaothanhcong > 0 then 'Tien Giao Thanh Cong'
	WHEN tienhuydon > 0  then 'Tien Huy Don'
	WHEN tiendonchuagiao > 0 then 'Tien Don Chua Giao'
	ELSE ''
END AS typetiendon

FROM trackingdebt_2
) AS a
-- 
-- WHERE
-- -- debtincharge = 'MDS'
-- -- AND
-- CASE
-- 	WHEN debtincharge = 'MDS' THEN
-- 		CASE
-- 			WHEN daydiff < 0 THEN 'no_xanh'
-- 			WHEN daydiff <= 5 THEN 'no_vang'
-- 			WHEN daydiff >5 and daydiff <=9 then 'no_do'
-- 			ELSE 'no_den'
-- 		END
-- 	WHEN debtincharge = 'CS' THEN
-- 		CASE
-- 			WHEN daydiff-dayterms < 0 THEN 'no_xanh'
-- 			WHEN daydiff-dayterms <= dayterms/2 THEN 'no_vang'
-- 			WHEN daydiff-dayterms > dayterms/2 AND daydiff-dayterms <= dayterms THEN 'no_do'
-- 			ELSE 'no_den'
-- 		END
-- END = 'no_xanh'
-- ORDER BY daydiff