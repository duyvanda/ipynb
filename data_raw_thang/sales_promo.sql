DROP TABLE IF EXISTS "biteam"."f_temp_ac_promo";

CREATE TABLE "biteam"."f_temp_ac_promo" (
  "id" SERIAL,
  "branchid" varchar(100) COLLATE "pg_catalog"."default" NOT NULL,
  "custid" varchar(100) COLLATE "pg_catalog"."default" NOT NULL,
  "totalamt" numeric(24)  
)
;

-- ----------------------------
-- Primary Key structure for table f_temp_sales_with_promo
-- ----------------------------
ALTER TABLE "biteam"."f_temp_ac_promo" ADD CONSTRAINT "f_temp_ac_promo_pkey" PRIMARY KEY ("id");