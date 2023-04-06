from project import *
import re

diffInQuery = {
    'Removed': ["s_suppkey = l_suppkey", "AND ps_suppkey = l_suppkey", "AND ps_partkey = l_partkey" ],
    'Modified': [[" AND s_acctbal > 10", " AND s_acctbal > 15"]],
    'Added': ["AND s_acctbal < 500"]
    # 'Added': ["AND c.c_name LIKE '%cheng'"]
}
# query_1 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
# query_2 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND c.c_name LIKE '%cheng'"


query_1 = "SELECT n_name, o_year, sum(amount) AS sum_profit FROM(SELECT n_name, DATE_PART('YEAR',o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name like '%green%' AND s_acctbal > 10 AND ps_supplycost > 100 ) AS profit GROUP BY n_name, o_year ORDER BY n_name, o_year desc"
query_2 = "SELECT n_name, o_year, sum(amount) AS sum_profit FROM(SELECT n_name, DATE_PART('YEAR',o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name like '%green%' AND s_acctbal > 15 AND ps_supplycost > 100 AND s_acctbal < 500) AS profit GROUP BY n_name, o_year ORDER BY n_name, o_year desc"

q_plan_1 = query(query_1)
q_plan_2 = query(query_2)

q_plan_1_nodes = QueryPlan(q_plan_1["Plan"])
q_plan_2_nodes = QueryPlan(q_plan_2["Plan"])
    
output = q_plan_1_nodes.IsEqual(q_plan_2_nodes)

Query = []
diffInQueryPlan = {
    'diffInJoinNode': [],
    'diffInScanNode': [],
    'diffInGroupNode': [],
    'diffInSortNode': [],
    'diffInPlan': []
}
queries_subset = []

#def explain(diffInQuery, diffInPlan):
def explain():
    queryplann(output)
    gettingAdd()
    table = joiningsplit()
    print(table)
    explaination = ""
    filter_exp = r"(?i)(?:AND|NOT|OR|WHERE)\s+(.*)"

    count = 0
    size = len(set(diffInQueryPlan["diffInJoinNode"]))
    size1 = len(set(diffInQueryPlan["diffInScanNode"]))
    for status, values in diffInQuery.items():
        if count == 1:
            explaination += "And with "
        else:
            explaination += "With "

        if status == "Added":
            explaination += "an addition of "
        elif status == "Removed":
            explaination += "the removal of "
        elif status == "Modified":
            explaination += "modification of "

        count1 = 0
        for value in values:
            if status == "Modified":
                if count1 == 0:
                    explaination += "join condition that changes from "
                if count1 % 2 == 0 and count1 != 0:
                    explaination += "and "
                    count1 = 0
                match_to = re.search(filter_exp, value[0])
                match_from = re.search(filter_exp, value[1])

                if match:
                    explaination += f"{match_to.group(1)} to {match_from.group(1)} "
                count1 += 2
            else:
                if count1 == 0:
                    explaination += "join condition such as "  
                elif count1 % 2 == 0:
                    explaination += "and "
                    count1 = 0

                match = re.search(filter_exp, value)
                if match:
                    explaination += f"{match.group(1)} "

                count1 += 1
        count = 1
        if status == "Added":
            explaination += ",extra filter are needed during "
            for i in (queries_subset):
                explaination += i
                if size1 == 1:
                    explaination += ". \n"
                else:
                    explaination += " and "
                    size1 -= 1
        elif status == "Removed":
            explaination += ",lesser to none filter are required for scans. "
        elif status == "Modified":
            explaination += ",no extra filtering is needed. "
    for i in set(diffInQueryPlan["diffInPlan"]):
        if i == "Gather Merge":
            explaination += "The query plan uses a Gather Merge that combines the output of its child nodes, which are executed by parallel workers. "
        elif i == "Gather":
            explaination += "The query plan uses a Gather operation to distribute the scanning of the table among two parallel workers. "
        elif i == "Hash":
            explaination += "Hashing have been used. "
        elif i == "Sort":
            explaination += "Sorting have been used. "
        elif i == "Materialize":
            explaination += "The query plan uses Materialization to the process of creating an intermediate result set. This maybe due to the size of the result set being too large to fit into memory or when multiple operations are performed on the same data. "
        elif i == "Memoize":
            explaination += "The query plan uses Memoize to improve the performance of recursive queries. "
        else:
            explaination += "The query plan uses " + i + ". "
    for i in set(diffInQueryPlan["diffInGroupNode"]):
            explaination += "\nGrouping techniqes such as " + i + "are used. "
    if size > 0:
        explaination += "\nSuch joined operation have been used: "
    for i in diffInQueryPlan["diffInJoinNode"]:
        for x in range(0,len(table),2):
            if x == 0:
                explaination += "\nTable " + table[x] + " and table " + table[x+1] + " is joined using " + i.split("-")[0] + "with join condition" + i.split("-")[1] + ". "
    for i in set(diffInQueryPlan["diffInSortNode"]):
        explaination += i + " ."
    explaination = explaination.replace("(", "").replace(")", "").replace("'", "").replace("::numeric", "").replace("~~", "LIKE").replace("::text", "")
    print(explaination)
    
def queryplann(output):
    if output[2] == None:
        print("No changes to query plan")
    else:
        Query.append("Changes: ")
        print_nodes(output[2])
    print("Different in Plan: ")
    for i in diffInQueryPlan:
        
        print(diffInQueryPlan[i])
    print("Different in Query: ")
    for i in diffInQuery:
        
        print(diffInQuery[i])
    
def print_nodes(node):
    Query.append(node.node)
    if type(node) == QueryPlanJoinNode and node.JoinCond != "":
            #print(node.JoinCond)
        diffInQueryPlan["diffInJoinNode"].append(node.node + " - " + node.JoinCond)
    if type(node) == QueryPlanScanNode:
        if node.Filter != "":
            diffInQueryPlan["diffInScanNode"].append(node.Filter + " - " + node.node +" on: " + node.RelationName)
    if type(node) == QueryPlanSortNode:
        diffInQueryPlan["diffInSortNode"].append(node.node)
    if type(node) == QueryPlanGroupNode:
        diffInQueryPlan["diffInGroupNode"].append(node.node)
    if type(node) == QueryPlanNode:
        diffInQueryPlan["diffInPlan"].append(node.node)
    if node.left:
        print_nodes(node.left)
    if node.right:
        print_nodes(node.right)

def get_table_name(text):
    # match the last word before the dot
    match = re.search(r"\b(\w+)\.", text)
    if match:
        return match.group(1)
    else:
        return None
    
def joiningsplit() -> list:
    table = []
    for i in diffInQueryPlan["diffInJoinNode"]:
        temp = i.split("-")[1]
        table.append(get_table_name(temp.split("=")[0].strip()))
        table.append(get_table_name(temp.split("=")[1].strip()))

    return(table)

def gettingAdd():
    global queries_subset
    matching_indices = []
    for i, s in enumerate(diffInQueryPlan["diffInScanNode"]):
        # print(s.replace("(", "").replace(")", "").replace("'", "").replace("~~", "LIKE").replace("::text", ""))
        for q in diffInQuery["Added"]:
            # print(q.split(".")[1].replace("'", ""))
            try:
                if q.split(".")[1].replace("'", "") in s.replace("(", "").replace(")", "").replace("'", "").replace("~~", "LIKE").replace("::text", ""):
                    matching_indices.append(i)
            except:
                if q.replace("'", "") in s.replace("(", "").replace(")", "").replace("'", "").replace("~~", "LIKE").replace("::text", ""):
                    matching_indices.append(i)

    queries_subset = [diffInQueryPlan["diffInScanNode"][i] for i in matching_indices]
    queries_subset = [s.split('-')[1].strip() for s in queries_subset]
    print(queries_subset)

explain()