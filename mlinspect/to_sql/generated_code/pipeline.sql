CREATE VIEW pg2_sf1_region_1_mlinid0_ctid AS (
	SELECT *, ctid AS pg2_sf1_region_ctid
	FROM pg2_sf1_region
);

	SELECT *, ctid AS pg2_sf1_nation_ctid
	FROM pg2_sf1_nation
);

	SELECT *, ctid AS pg1_sf1_supplier_ctid
	FROM pg1_sf1_supplier
);

	SELECT *, ctid AS pg1_sf1_orders_ctid
	FROM pg1_sf1_orders
);

	SELECT *, ctid AS pg2_sf1_lineitem_ctid
	FROM pg2_sf1_lineitem
);

	SELECT *, ctid AS pg1_sf1_customer_ctid
	FROM pg1_sf1_customer
);

	SELECT "r_name", pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid
);

	SELECT ("r_name") = ('ASIA') AS op_8, pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid
);

	SELECT "r_regionkey", "r_name", "r_comment", pg2_sf1_region_ctid
	FROM pg2_sf1_region_1_mlinid0_ctid 
	WHERE ("r_name") = ('ASIA')
);

	SELECT tb1."n_nationkey", tb1."n_name", tb1."n_regionkey", tb1."n_comment", tb1.pg2_sf1_nation_ctid, tb2."r_regionkey", tb2."r_name", tb2."r_comment", tb2.pg2_sf1_region_ctid
	FROM pg2_sf1_nation_2_mlinid1_ctid tb1 
	INNER JOIN block_mlinid8_10 tb2 ON tb1."n_regionkey" = tb2."r_regionkey"
);

	SELECT "n_nationkey", "n_name", pg2_sf1_nation_ctid, pg2_sf1_region_ctid
	FROM block_mlinid9_11
);

	SELECT "s_suppkey", "s_nationkey", pg1_sf1_supplier_ctid
	FROM pg1_sf1_supplier_3_mlinid2_ctid
);

	SELECT tb1."s_suppkey", tb1."s_nationkey", tb1.pg1_sf1_supplier_ctid, tb2."n_nationkey", tb2."n_name", tb2.pg2_sf1_nation_ctid, tb2.pg2_sf1_region_ctid
	FROM block_mlinid11_13 tb1 
	INNER JOIN block_mlinid10_12 tb2 ON tb1."s_nationkey" = tb2."n_nationkey"
);

	SELECT "s_suppkey", "s_nationkey", "n_name", pg2_sf1_nation_ctid, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid
	FROM block_mlinid12_14
);

	SELECT "l_suppkey", "l_orderkey", "l_extendedprice", "l_discount", pg2_sf1_lineitem_ctid
	FROM pg2_sf1_lineitem_5_mlinid4_ctid
);

	SELECT tb1."l_suppkey", tb1."l_orderkey", tb1."l_extendedprice", tb1."l_discount", tb1.pg2_sf1_lineitem_ctid, tb2."s_suppkey", tb2."s_nationkey", tb2."n_name", tb2.pg2_sf1_nation_ctid, tb2.pg2_sf1_region_ctid, tb2.pg1_sf1_supplier_ctid
	FROM block_mlinid14_16 tb1 
	INNER JOIN block_mlinid13_15 tb2 ON tb1."l_suppkey" = tb2."s_suppkey"
);

	SELECT "o_orderkey", "o_custkey", "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT ("o_orderdate") >= ('1994-01-01') AS op_20, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT ("o_orderdate") < ('1995-01-01') AS op_23, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT ((("o_orderdate") >= ('1994-01-01')) AND (("o_orderdate") < ('1995-01-01'))) AS op_25, pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid
);

	SELECT "o_orderkey", "o_custkey", "o_orderdate", pg1_sf1_orders_ctid
	FROM pg1_sf1_orders_4_mlinid3_ctid 
	WHERE (("o_orderdate") >= ('1994-01-01')) AND (("o_orderdate") < ('1995-01-01'))
);

	SELECT "o_orderkey", "o_custkey", pg1_sf1_orders_ctid
	FROM block_mlinid22_27
);

	SELECT "c_custkey", "c_nationkey", pg1_sf1_customer_ctid
	FROM pg1_sf1_customer_6_mlinid5_ctid
);

	SELECT tb1."o_orderkey", tb1."o_custkey", tb1.pg1_sf1_orders_ctid, tb2."c_custkey", tb2."c_nationkey", tb2.pg1_sf1_customer_ctid
	FROM block_mlinid23_28 tb1 
	INNER JOIN block_mlinid24_29 tb2 ON tb1."o_custkey" = tb2."c_custkey"
);

	SELECT "o_orderkey", "c_nationkey", pg1_sf1_orders_ctid, pg1_sf1_customer_ctid
	FROM block_mlinid25_30
);

	SELECT tb1."l_suppkey", tb1."l_orderkey", tb1."l_extendedprice", tb1."l_discount", tb1."s_suppkey", tb1."s_nationkey", tb1."n_name", tb1.pg2_sf1_nation_ctid, tb1.pg2_sf1_region_ctid, tb1.pg2_sf1_lineitem_ctid, tb1.pg1_sf1_supplier_ctid, tb2."o_orderkey", tb2."c_nationkey", tb2.pg1_sf1_orders_ctid, tb2.pg1_sf1_customer_ctid
	FROM block_mlinid15_17 tb1 
	INNER JOIN block_mlinid26_31 tb2 ON tb1."l_orderkey" = tb2."o_orderkey" AND tb1."s_nationkey" = tb2."c_nationkey"
);

	SELECT "l_extendedprice", "l_discount", "n_name", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid27_32
);

	SELECT "l_extendedprice", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);

	SELECT "l_discount", pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);

	SELECT (1) - ("l_discount") AS op_36, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);

	SELECT (("l_extendedprice") * ((1) - ("l_discount"))) AS op_38, pg2_sf1_region_ctid, pg1_sf1_supplier_ctid, pg2_sf1_nation_ctid, pg1_sf1_customer_ctid, pg2_sf1_lineitem_ctid, pg1_sf1_orders_ctid
	FROM block_mlinid28_33
);

	SELECT *, ("l_extendedprice") * ((1) - ("l_discount")) AS volume
	FROM block_mlinid28_33
);

	SELECT "n_name", SUM("volume") AS "volume" 
	FROM block_mlinid33_40
	GROUP BY "n_name"
);

	SELECT "volume", "n_name"
	FROM block_mlinid34_41
	ORDER BY volume DESC
);

SELECT "volume", "n_name" FROM block_mlinid36_42;