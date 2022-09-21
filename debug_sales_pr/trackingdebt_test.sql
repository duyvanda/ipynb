/*
 Navicat Premium Data Transfer

 Source Server         : biteam
 Source Server Type    : PostgreSQL
 Source Server Version : 130004
 Source Host           : 171.235.26.161:5432
 Source Catalog        : biteam
 Source Schema         : biteam

 Target Server Type    : PostgreSQL
 Target Server Version : 130004
 File Encoding         : 65001

 Date: 12/10/2021 12:34:08
*/


-- ----------------------------
-- Table structure for trackingdebt_test
-- ----------------------------
DROP TABLE IF EXISTS "biteam"."trackingdebt_test";
CREATE TABLE "biteam"."trackingdebt_test" (
  "ordernbr" varchar(100) COLLATE "pg_catalog"."default" NOT NULL,
  "branchid" varchar(100) COLLATE "pg_catalog"."default" NOT NULL,
  "position" varchar(100) COLLATE "pg_catalog"."default",
  "slsperid" varchar(100) COLLATE "pg_catalog"."default",
  "supname" varchar(100) COLLATE "pg_catalog"."default",
  "asmname" varchar(100) COLLATE "pg_catalog"."default",
  "rsmname" varchar(100) COLLATE "pg_catalog"."default",
  "dateoforder" timestamp(6) NOT NULL,
  "duedate" timestamp(6) NOT NULL,
  "slspername" varchar(100) COLLATE "pg_catalog"."default",
  "debtincharge" varchar(100) COLLATE "pg_catalog"."default",
  "terms" varchar(100) COLLATE "pg_catalog"."default",
  "paymentsform" varchar(100) COLLATE "pg_catalog"."default",
  "termstype" varchar(100) COLLATE "pg_catalog"."default",
  "territory" varchar(100) COLLATE "pg_catalog"."default",
  "state" varchar(100) COLLATE "pg_catalog"."default",
  "vptt" varchar(100) COLLATE "pg_catalog"."default",
  "deliveryunit" varchar(100) COLLATE "pg_catalog"."default",
  "subchannel" varchar(100) COLLATE "pg_catalog"."default",
  "tiennodauky" numeric(24),
  "tienchotso" float4,
  "tiengiaothanhcong" float4,
  "tienhuydon" float4,
  "tienlenbangke" float4,
  "tienthuquyxacnhan" float4,
  "dondauky" int4,
  "donchotso" int4,
  "dongiaothanhcong" int4,
  "donhuy" int4,
  "donlenbangke" int4,
  "donthuquyxacnhan" int4,
  "tiennocongty" float4,
  "donnocongty" int4,
  "donchuagiao" int4,
  "tiendonchuagiao" float4,
  "overduedate" bool,
  "nodenhan" float4,
  "nochuadenhan" float4
)
;

-- ----------------------------
-- Primary Key structure for table trackingdebt_test
-- ----------------------------
ALTER TABLE "biteam"."trackingdebt_test" ADD CONSTRAINT "trackingdebt_test_pkey" PRIMARY KEY ("ordernbr", "branchid", "duedate", "dateoforder");
