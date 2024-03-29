#import dask
#import modin.pandas as pd
import pandas as pd
import numpy as np
import timeit
import csv
#import connectorx as cx
#import modin.config as modin_cfg
import datetime
from sqlalchemy import create_engine
import time


def udf_disc_price(extended, discount):
    return np.multiply(extended, np.subtract(1, discount))


def udf_charge(extended, discount, tax):
    return np.multiply(extended, np.multiply(np.subtract(1, discount), np.add(1, tax)))


def load_table(method, table, sf):

    if method == 'psycopg2':
        alchemyEngine = create_engine(
            'postgresql+psycopg2://postgres:123456@127.0.0.1:25432/db1', pool_recycle=3600)
        parse_dates = []
        if table == 'orders':
            parse_dates = ['o_orderdate']
        elif table == 'lineitem':
            parse_dates = ['l_shipdate', 'l_commitdate', 'l_receiptdate']

        df = pd.read_sql('SELECT * FROM pg2_sf'+str(sf)+'_'+table+ " limit 10",
                         alchemyEngine, parse_dates=parse_dates)
        print("hallelujah mal schaun ob das df alle daten hat",df)
    # elif method == 'connectorx':
    #    df = cx.read_sql('postgresql://postgres:123456@localhost:15432/db1',
    #                     'SELECT * FROM '+'pg1_sf'+str(sf)+'_'+table)
    else:
        df = pd.DataFrame

    cols = df.select_dtypes(object).columns
    df[cols] = df[cols].apply(lambda x: x.astype(str).str.strip())
    return df


def run(q, read_method, sf):
    if q == 1:
        t0 = time.time()
        a = datetime.datetime.now()
        lineitem = load_table(read_method, 'lineitem', sf)

        b = datetime.datetime.now()
        t1 = time.time()
        loadt = (b-a).total_seconds() * 1000
        df = lineitem[["l_shipdate", "l_returnflag", "l_linestatus", "l_quantity",
                       "l_extendedprice", "l_discount", "l_tax"]][(lineitem['l_shipdate'] <= '1998-09-01')]
        df['disc_price'] = udf_disc_price(
            df['l_extendedprice'], df['l_discount'])
        df['charge'] = udf_charge(df['l_extendedprice'],
                                  df['l_discount'], df['l_tax'])
        res = df.groupby(['l_returnflag', 'l_linestatus'])\
            .agg({'l_quantity': 'sum', 'l_extendedprice': 'sum', 'disc_price': 'sum', 'charge': 'sum',
                  'l_quantity': 'mean', 'l_extendedprice': 'mean', 'l_discount': 'mean', 'l_shipdate': 'count'})
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        t2 = time.time()
        print("\nTime spend with original (pandas): " + str(t1 - t0))
        print("time spend withhout loading table "+ str(t2-t1))
        #del lineitem
        #print("hier ist df", df)
        print("res", res)
        return loadt, exect, res
    if q == 2:
        a = datetime.datetime.now()
        nation = load_table(read_method, 'nation', sf)
        part = load_table(read_method, 'part', sf)
        supplier = load_table(read_method, 'supplier', sf)
        region = load_table(read_method, 'region', sf)
        partsupp = load_table(read_method, 'partsupp', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        ps = partsupp[["ps_partkey", "ps_suppkey", "ps_supplycost"]]
        p = part[["p_partkey", "p_mfgr", "p_size", "p_type"]][(part.p_size == 15) & (
            part.p_type.str.match(".*BRASS$"))][["p_partkey", "p_mfgr"]]
        psp = ps.merge(p, left_on="ps_partkey", right_on="p_partkey")
        s = supplier[["s_suppkey", "s_nationkey", "s_acctbal",
                      "s_name", "s_address", "s_phone", "s_comment"]]
        psps = psp.merge(s, left_on="ps_suppkey", right_on="s_suppkey")[
            ["ps_partkey", "ps_supplycost", "p_mfgr", "s_nationkey",         "s_acctbal", "s_name", "s_address", "s_phone", "s_comment"]]
        nr = nation.merge(region[region.r_name == "EUROPE"], left_on="n_regionkey",
                          right_on="r_regionkey")[["n_nationkey", "n_name"]]
        pspsnr = psps.merge(nr, left_on="s_nationkey", right_on="n_nationkey")[
            ["ps_partkey", "ps_supplycost", "p_mfgr", "n_name", "s_acctbal", "s_name", "s_address", "s_phone", "s_comment"]]
        aggr = pspsnr.groupby("ps_partkey").agg(
            {'ps_supplycost': min}).reset_index()
        sj = pspsnr.merge(aggr, left_on=["ps_partkey", "ps_supplycost"], right_on=[
            "ps_partkey", "ps_supplycost"])
        res = sj[["s_acctbal", "s_name", "n_name", "ps_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"]].sort_values(
            ["s_acctbal", "n_name", "s_name", "ps_partkey"], ascending=[False, True, True, True]).head(100)
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del partsupp, part, supplier, region, nation
        return loadt, exect, res

    if q == 3:
        a = datetime.datetime.now()
        orders = load_table(read_method, 'orders', sf)
        customer = load_table(read_method, 'customer', sf)
        lineitem = load_table(read_method, 'lineitem', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000
        o = orders[["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]][orders.o_orderdate <
                                                                                 "1995-03-15"][["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"]]
        c = customer[["c_custkey", "c_mktsegment"]][customer.c_mktsegment ==
                                                    "BUILDING"][["c_custkey", "c_mktsegment"]]
        oc = o.merge(c, left_on="o_custkey", right_on="c_custkey")[
            ["o_orderkey", "o_orderdate", "o_shippriority"]]
        l = lineitem[["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"]
                     ][lineitem.l_shipdate > "1995-03-15"][["l_orderkey", "l_extendedprice", "l_discount"]]
        loc = l.merge(oc, left_on="l_orderkey", right_on="o_orderkey")
        loc["volume"] = loc.l_extendedprice * (1 - loc.l_discount)
        res = loc.groupby(["l_orderkey", "o_orderdate", "o_shippriority"]).agg({'volume': sum}).reset_index()[
            ["l_orderkey", "volume", "o_orderdate", "o_shippriority"]].sort_values(["volume", "o_orderdate"], ascending=[False, True]).head(10)
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del orders, customer, lineitem
        return loadt, exect, res
    if q == 4:
        a = datetime.datetime.now()
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        l = lineitem[["l_orderkey", "l_commitdate"]
                     ][lineitem.l_commitdate < lineitem.l_receiptdate][["l_orderkey"]]
        o = orders[["o_orderkey", "o_orderpriority", "o_orderdate"]][(
            orders.o_orderdate >= "1993-07-01") & (orders.o_orderdate < "1993-10-01")][["o_orderkey", "o_orderpriority"]]
        lo = l.merge(o, left_on="l_orderkey", right_on="o_orderkey").drop_duplicates()[
            ["o_orderpriority"]]
        res = lo.groupby("o_orderpriority").size().reset_index(
            name='counts').sort_values('o_orderpriority')
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del orders, lineitem
        return loadt, exect, res
    if q == 5:
        a = datetime.datetime.now()
        region = load_table(read_method, 'region', sf)
        nation = load_table(read_method, 'nation', sf)
        supplier = load_table(read_method, 'supplier', sf)
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)
        customer = load_table(read_method, 'customer', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        nr = nation.merge(region[region.r_name == "ASIA"], left_on="n_regionkey",
                          right_on="r_regionkey")[["n_nationkey", "n_name"]]
        snr = supplier[["s_suppkey", "s_nationkey"]].merge(
            nr, left_on="s_nationkey", right_on="n_nationkey")[["s_suppkey", "s_nationkey", "n_name"]]
        lsnr = lineitem[["l_suppkey", "l_orderkey", "l_extendedprice", "l_discount"]].merge(
            snr, left_on="l_suppkey", right_on="s_suppkey")
        o = orders[["o_orderkey", "o_custkey", "o_orderdate"]][(
            orders.o_orderdate >= "1994-01-01") & (orders.o_orderdate < "1995-01-01")][["o_orderkey", "o_custkey"]]
        oc = o.merge(customer[["c_custkey", "c_nationkey"]], left_on="o_custkey",
                     right_on="c_custkey")[["o_orderkey", "c_nationkey"]]
        lsnroc = lsnr.merge(oc, left_on=["l_orderkey", "s_nationkey"], right_on=[
                            "o_orderkey", "c_nationkey"])[["l_extendedprice", "l_discount", "n_name"]]
        lsnroc["volume"] = lsnroc.l_extendedprice * (1 - lsnroc.l_discount)
        res = lsnroc.groupby("n_name").agg(
            {'volume': sum}).reset_index().sort_values("volume", ascending=False)
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del nation, supplier, orders, lineitem, customer
        return loadt, exect, res
    if q == 6:
        a = datetime.datetime.now()
        lineitem = load_table(read_method, 'lineitem', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        l = lineitem[["l_extendedprice", "l_discount", "l_shipdate", "l_quantity"]][
            (lineitem.l_shipdate >= "1994-01-01") &
            (lineitem.l_shipdate < "1995-01-01") &
            (lineitem.l_discount >= 0.05) &
            (lineitem.l_discount <= 0.07) &
            (lineitem.l_quantity < 24)][["l_extendedprice", "l_discount"]]
        res = (l.l_extendedprice * l.l_discount).sum()
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del lineitem
        return loadt, exect, res
    if q == 7:
        a = datetime.datetime.now()
        nation = load_table(read_method, 'nation', sf)
        supplier = load_table(read_method, 'supplier', sf)
        customer = load_table(read_method, 'customer', sf)
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        sn = supplier[["s_nationkey", "s_suppkey"]].merge(nation[["n_nationkey", "n_name"]][(nation.n_name == "FRANCE") | (
            nation.n_name == "GERMANY")], left_on="s_nationkey", right_on="n_nationkey")[["s_suppkey", "n_name"]]
        sn.columns = ["s_suppkey", "n1_name"]
        cn = customer[["c_custkey", "c_nationkey"]].merge(nation[["n_nationkey", "n_name"]][(nation.n_name == "FRANCE") | (
            nation.n_name == "GERMANY")], left_on="c_nationkey", right_on="n_nationkey")[["c_custkey", "n_name"]]
        cn.columns = ["c_custkey", "n2_name"]
        cno = orders[["o_custkey", "o_orderkey"]].merge(
            cn, left_on="o_custkey", right_on="c_custkey")[["o_orderkey", "n2_name"]]
        cnol = lineitem[["l_orderkey", "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"]][(lineitem.l_shipdate >= "1995-01-01") & (lineitem.l_shipdate <= "1996-12-31")][[
            "l_orderkey", "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"]].merge(cno, left_on="l_orderkey", right_on="o_orderkey")[["l_suppkey", "l_shipdate", "l_extendedprice", "l_discount", "n2_name"]]
        cnolsn = cnol.merge(sn, left_on="l_suppkey", right_on="s_suppkey")
        cnolsn["volume"] = cnolsn.l_extendedprice * (1 - cnolsn.l_discount)
        cnolsn["l_year"] = cnolsn.l_shipdate.dt.year
        cnolsnf = cnolsn[((cnolsn.n1_name == "FRANCE") & (cnolsn.n2_name == "GERMANY")) | (
            (cnolsn.n1_name == "GERMANY") & (cnolsn.n2_name == "FRANCE"))]
        res = cnolsnf[["n1_name", "n2_name", "l_year", "volume"]].groupby(["n1_name", "n2_name", "l_year"]).agg(
            {'volume': sum}).reset_index().sort_values(["n1_name", "n2_name", "l_year"])
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del nation, supplier, customer, orders, lineitem
        return loadt, exect, res
    if q == 8:
        a = datetime.datetime.now()
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)
        nation = load_table(read_method, 'nation', sf)
        supplier = load_table(read_method, 'supplier', sf)
        customer = load_table(read_method, 'customer', sf)
        part = load_table(read_method, 'part', sf)
        region = load_table(read_method, 'region', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        nr = nation.merge(region[region.r_name == "AMERICA"],
                          left_on="n_regionkey", right_on="r_regionkey")[["n_nationkey"]]
        cnr = customer[["c_custkey", "c_nationkey"]].merge(
            nr, left_on="c_nationkey", right_on="n_nationkey")[["c_custkey"]]
        ocnr = orders[["o_orderkey", "o_custkey", "o_orderdate"]][(orders.o_orderdate >= "1995-01-01") & (
            orders.o_orderdate <= "1996-12-31")].merge(cnr, left_on="o_custkey", right_on="c_custkey")[["o_orderkey", "o_orderdate"]]
        locnr = lineitem[["l_orderkey", "l_partkey", "l_suppkey", "l_extendedprice", "l_discount"]].merge(
            ocnr, left_on="l_orderkey", right_on="o_orderkey")[["l_partkey", "l_suppkey", "l_extendedprice", "l_discount", "o_orderdate"]]
        p = part[["p_partkey", "p_type"]][part.p_type ==
                                          "ECONOMY ANODIZED STEEL"][["p_partkey"]]
        locnrp = locnr.merge(p, left_on="l_partkey", right_on="p_partkey")[
            ["l_suppkey", "l_extendedprice", "l_discount", "o_orderdate"]]
        locnrps = locnrp.merge(supplier[["s_suppkey", "s_nationkey"]], left_on="l_suppkey", right_on="s_suppkey")[
            ["l_extendedprice", "l_discount", "o_orderdate", "s_nationkey"]]
        locnrpsn = locnrps.merge(nation[["n_nationkey", "n_name"]], left_on="s_nationkey", right_on="n_nationkey")[
            ["l_extendedprice", "l_discount", "o_orderdate", "n_name"]]
        locnrpsn["volume"] = locnrpsn.l_extendedprice * \
            (1 - locnrpsn.l_discount)
        locnrpsn["o_year"] = locnrpsn.o_orderdate.dt.year

        def udf(df):
            demonimator = df["volume"].sum()
            df = df[df["n_name"] == "BRAZIL"]
            numerator = df["volume"].sum()
            return numerator / demonimator

        res = locnrpsn[["o_year", "volume", "n_name"]].groupby("o_year", group_keys=True).apply(
            udf).sort_values(ascending=True)

        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del orders, lineitem, nation, supplier, customer, part, region
        return loadt, exect, res
    if q == 9:
        a = datetime.datetime.now()
        part = load_table(read_method, 'part', sf)
        partsupp = load_table(read_method, 'partsupp', sf)
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)
        nation = load_table(read_method, 'nation', sf)
        supplier = load_table(read_method, 'supplier', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        p = part[["p_partkey", "p_name"]
                 ][part.p_name.str.match(".*green.*")][["p_partkey"]]
        psp = partsupp[["ps_suppkey", "ps_partkey", "ps_supplycost"]].merge(
            p, left_on="ps_partkey", right_on="p_partkey")
        sn = supplier[["s_suppkey", "s_nationkey"]].merge(
            nation[["n_nationkey", "n_name"]], left_on="s_nationkey", right_on="n_nationkey")[["s_suppkey", "n_name"]]
        pspsn = psp.merge(sn, left_on="ps_suppkey", right_on="s_suppkey")
        lpspsn = lineitem[["l_suppkey", "l_partkey", "l_orderkey", "l_extendedprice", "l_discount", "l_quantity"]].merge(
            pspsn, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"])[["l_orderkey", "l_extendedprice", "l_discount", "l_quantity", "ps_supplycost", "n_name"]]
        olpspsn = orders[["o_orderkey", "o_orderdate"]].merge(lpspsn, left_on="o_orderkey", right_on="l_orderkey")[
            ["l_extendedprice", "l_discount", "l_quantity", "ps_supplycost", "n_name", "o_orderdate"]]
        olpspsn["amount"] = olpspsn.l_extendedprice * \
            (1 - olpspsn.l_discount) - olpspsn.ps_supplycost * olpspsn.l_quantity
        olpspsn["o_year"] = olpspsn.o_orderdate.dt.year
        res = olpspsn[["n_name", "o_year", "amount"]].groupby(["n_name", "o_year"]).agg(
            {'amount': sum}).reset_index().sort_values(["n_name", "o_year"], ascending=[True, False])
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del part, partsupp, orders, lineitem, nation, supplier
        return loadt, exect, res
    if q == 10:
        a = datetime.datetime.now()
        orders = load_table(read_method, 'orders', sf)
        lineitem = load_table(read_method, 'lineitem', sf)
        customer = load_table(read_method, 'customer', sf)
        nation = load_table(read_method, 'nation', sf)

        b = datetime.datetime.now()
        loadt = (b-a).total_seconds() * 1000

        l = lineitem[["l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"]
                     ][lineitem.l_returnflag == "R"][["l_orderkey", "l_extendedprice", "l_discount"]]
        o = orders[["o_orderkey", "o_custkey", "o_orderdate"]][(
            orders.o_orderdate >= "1993-10-01") & (orders.o_orderdate < "1994-01-01")][["o_orderkey", "o_custkey"]]
        lo = l.merge(o, left_on="l_orderkey", right_on="o_orderkey")[
            ["l_extendedprice", "l_discount", "o_custkey"]]
        lo["volume"] = lo.l_extendedprice * (1 - lo.l_discount)
        lo_aggr = lo.groupby("o_custkey").agg({'volume': sum}).reset_index()
        c = customer[["c_custkey", "c_nationkey", "c_name",
                      "c_acctbal", "c_phone", "c_address", "c_comment"]]
        loc = lo_aggr.merge(c, left_on="o_custkey", right_on="c_custkey")
        locn = loc.merge(nation[["n_nationkey", "n_name"]],
                         left_on="c_nationkey", right_on="n_nationkey")
        res = locn[["o_custkey", "c_name", "volume", "c_acctbal", "n_name", "c_address",
                    "c_phone", "c_comment"]].sort_values("volume", ascending=False).head(20)
        exect = (datetime.datetime.now()-b).total_seconds() * 1000
        del orders, lineitem, customer, nation
        return loadt, exect, res


def main():
    f = open("pandas.csv", 'a')
    writer = csv.writer(f)
    print("HEY")
    dfnew=[1,2,3,4]
    for sf in [1]:
        print("n1")
        q = 1
        for rm in ['psycopg2']:
            print("n3")
            for r in range(1):
                print("nay")
                loadt, exect, res = run(q, rm, sf)
                #writer.writerow(["pandas", "%d" % q, rm,
                #                 "%d" % sf, "%f" % loadt, "%f" % exect, "%d" % r])
                #f.flush()


#if __name__ == '__main__':
#    main()
#    dfnew2=[1,2,3,4]
    
    
    
    
t0 = time.time()
a = datetime.datetime.now()
lineitem = load_table('psycopg2', 'lineitem', 1)

b = datetime.datetime.now()
t1 = time.time()
loadt = (b-a).total_seconds() * 1000
df = lineitem[["l_shipdate", "l_returnflag", "l_linestatus", "l_quantity",
               "l_extendedprice", "l_discount", "l_tax"]][(lineitem['l_shipdate'] <= '1998-09-01')]
df['disc_price'] = udf_disc_price(
    df['l_extendedprice'], df['l_discount'])
df['charge'] = udf_charge(df['l_extendedprice'],
                          df['l_discount'], df['l_tax'])
res = df.groupby(['l_returnflag', 'l_linestatus'])\
    .agg({'l_quantity': 'sum', 'l_extendedprice': 'sum', 'disc_price': 'sum', 'charge': 'sum',
          'l_quantity': 'mean', 'l_extendedprice': 'mean', 'l_discount': 'mean', 'l_shipdate': 'count'})
exect = (datetime.datetime.now()-b).total_seconds() * 1000
t2 = time.time()
print("\nTime spend with original (pandas): " + str(t1 - t0))
print("time spend withhout loading table "+ str(t2-t1))
#del lineitem
#print("hier ist df", df)
print("res", res)