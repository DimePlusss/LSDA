CREATE VIEW pg2_sf1_region_1_mlinid0_ctid AS (
	SELECT *, ctid AS pg2_sf1_region_ctid
	FROM pg2_sf1_region
);
CREATE VIEW pg2_sf1_nation_2_mlinid1_ctid AS (
	SELECT *, ctid AS pg2_sf1_nation_ctid
	FROM pg2_sf1_nation
);
CREATE VIEW pg1_sf1_supplier_3_mlinid2_ctid AS (
	SELECT *, ctid AS pg1_sf1_supplier_ctid
	FROM pg1_sf1_supplier
);
CREATE VIEW pg1_sf1_orders_4_mlinid3_ctid AS (
	SELECT *, ctid AS pg1_sf1_orders_ctid
	FROM pg1_sf1_orders
);
CREATE VIEW pg2_sf1_lineitem_5_mlinid4_ctid AS (
	SELECT *, ctid AS pg2_sf1_lineitem_ctid
	FROM pg2_sf1_lineitem
);
CREATE VIEW pg1_sf1_customer_6_mlinid5_ctid AS (
	SELECT *, ctid AS pg1_sf1_customer_ctid
	FROM pg1_sf1_customer
);
CREATE VIEW block_mlinid6_7 AS (
	SELECT "r_name", pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid
);
CREATE VIEW block_mlinid7_9 AS (
	SELECT ("r_name") = ('ASIA') AS op_8, pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid
);
CREATE VIEW block_mlinid8_10 AS (
	SELECT "r_regionkey", "r_name", "r_comment", pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid 
	WHERE ("r_name") = ('ASIA')
);
CREATE VIEW block_mlinid9_11 AS (
	SELECT tb1."n_nationkey", tb1."n_name", tb1."n_regionkey", tb1."n_comment", tb1.pg2_sf1_nation_ctid, tb2."r_regionkey", tb2."r_name", tb2."r_comment", tb2.pg2_sf1_region_ctid
	FROM pg2_sf1_nation_2_mlinid1_ctid tb1 
	INNER JOIN block_mlinid8_10 tb2 ON tb1."n_regionkey" = tb2."r_regionkey"
);
CREATE VIEW block_mlinid10_12 AS (
	SELECT "n_nationkey", "n_name", pg2_sf1_nation_ctid, pg2_sf1_region_ctid
	FROM block_mlinid9_11
);
CREATE VIEW block_mlinid11_13 AS (
	SELECT "s_suppkey", "s_nationkey", pg1_sf1_supplier_ctid
	FROM pg1_sf1_supplier_3_mlinid2_ctid
);
CREATE VIEW block_mlinid12_14 AS (
	SELECT tb1."s_suppkey", tb1."s_nationkey", tb1.pg1_sf1_supplier_ctid, tb2."n_nationkey", tb2."n_name", tb2.pg2_sf1_nation_ctid, tb2.pg2_sf1_region_ctid
	FROM block_mlinid11_13 tb1 
	INNER JOIN block_mlinid10_12 tb2 ON tb1."s_nationkey" = tb2."n_nationkey"
);
CREATE VIEW block_mlinid13_15 AS (
	SELECT "s_suppkey", "s_nationkey", "n_name", pg2_sf1_nation_ctid, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid
	FROM block_mlinid12_14
);
CREATE VIEW block_mlinid14_16 AS (
	SELECT "l_suppkey", "l_orderkey", "l_extendedprice", "l_discount", pg2_sf1_lineitem_ctid
	FROM pg2_sf1_lineitem_5_mlinid4_ctid
);
CREATE VIEW block_mlinid15_17 AS (
	SELECT tb1."l_suppkey", tb1."l_orderkey", tb1."l_extendedprice", tb1."l_discount", tb1.pg2_sf1_lineitem_ctid, tb2."s_suppkey", tb2."s_nationkey", tb2."n_name", tb2.pg2_sf1_nation_ctid, tb2.pg2_sf1_region_ctid, tb2.pg1_sf1_supplier_ctid
	FROM block_mlinid14_16 tb1 
	INNER JOIN block_mlinid13_15 tb2 ON tb1."l_suppkey" = tb2."s_suppkey"
);
CREATE VIEW block_mlinid16_18 AS (
	SELECT "o_orderkey", "o_custkey", "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid17_19 AS (
	SELECT "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid18_21 AS (
	SELECT ("o_orderdate") >= ('1994-01-01') AS op_20, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid19_22 AS (
	SELECT "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid20_24 AS (
	SELECT ("o_orderdate") < ('1995-01-01') AS op_23, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid21_26 AS (
	SELECT ((("o_orderdate") >= ('1994-01-01')) AND (("o_orderdate") < ('1995-01-01'))) AS op_25, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);
CREATE VIEW block_mlinid22_27 AS (
	SELECT "o_orderkey", "o_custkey", "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid 
	WHERE (("o_orderdate") >= ('1994-01-01')) AND (("o_orderdate") < ('1995-01-01'))
);
CREATE VIEW block_mlinid23_28 AS (
	SELECT "o_orderkey", "o_custkey", pg1_sf1_orders_ctid
	FROM block_mlinid22_27
);
CREATE VIEW block_mlinid24_29 AS (
	SELECT "c_custkey", "c_nationkey", pg1_sf1_customer_ctid
	FROM pg1_sf1_customer_6_mlinid5_ctid
);
CREATE VIEW block_mlinid25_30 AS (
	SELECT tb1."o_orderkey", tb1."o_custkey", tb1.pg1_sf1_orders_ctid, tb2."c_custkey", tb2."c_nationkey", tb2.pg1_sf1_customer_ctid
	FROM block_mlinid23_28 tb1 
	INNER JOIN block_mlinid24_29 tb2 ON tb1."o_custkey" = tb2."c_custkey"
);
CREATE VIEW block_mlinid26_31 AS (
	SELECT "o_orderkey", "c_nationkey", pg1_sf1_orders_ctid, pg1_sf1_customer_ctid
	FROM block_mlinid25_30
);
CREATE VIEW block_mlinid27_32 AS (
	SELECT tb1."l_suppkey", tb1."l_orderkey", tb1."l_extendedprice", tb1."l_discount", tb1."s_suppkey", tb1."s_nationkey", tb1."n_name", tb1.pg2_sf1_nation_ctid, tb1.pg2_sf1_region_ctid, tb1.pg2_sf1_lineitem_ctid, tb1.pg1_sf1_supplier_ctid, tb2."o_orderkey", tb2."c_nationkey", tb2.pg1_sf1_orders_ctid, tb2.pg1_sf1_customer_ctid
	FROM block_mlinid15_17 tb1 
	INNER JOIN block_mlinid26_31 tb2 ON tb1."l_orderkey" = tb2."o_orderkey" AND tb1."s_nationkey" = tb2."c_nationkey"
);
CREATE VIEW block_mlinid28_33 AS (
	SELECT "l_extendedprice", "l_discount", "n_name", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid27_32
);
CREATE VIEW block_mlinid29_34 AS (
	SELECT "l_extendedprice", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);
CREATE VIEW block_mlinid30_35 AS (
	SELECT "l_discount", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);
CREATE VIEW block_mlinid31_37 AS (
	SELECT (1) - ("l_discount") AS op_36, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);
CREATE VIEW block_mlinid32_39 AS (
	SELECT (("l_extendedprice") * ((1) - ("l_discount"))) AS op_38, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);
CREATE VIEW block_mlinid33_40 AS (
	SELECT *, ("l_extendedprice") * ((1) - ("l_discount")) AS volume
	FROM block_mlinid28_33
);
CREATE VIEW block_mlinid34_41 AS (
	SELECT "n_name", SUM("volume") AS "volume" 
	FROM block_mlinid33_40
	GROUP BY "n_name"
);
CREATE VIEW block_mlinid36_42 AS (
	SELECT "volume", "n_name"
	FROM block_mlinid34_41
	ORDER BY volume DESC
);

SELECT "volume", "n_name" FROM block_mlinid36_42;