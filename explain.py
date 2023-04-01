from project import *
import re

diffInQuery = {
    'Removed': ["s_suppkey = l_suppkey", "AND ps_suppkey = l_suppkey", "AND ps_partkey = l_partkey" ],
    'Modified': [[" AND s_acctbal > 10", " AND s_acctbal > 15"]],
    'Added': ["AND s_acctbal < 500"]
    #'Added': ["AND c.c_name LIKE '%cheng'"]
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

changes = []
original = []
toggle = 0
#def explain(diffInQuery, diffInPlan):
def explain():
    queryplann(output)
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
                if count1 % 2 == 1:
                    explaination += "and "
                    count1 = 0

                match = re.search(filter_exp, value)
                if match:
                    explaination += f"{match.group(1)} "

                count1 += 1
        count = 1
        if status == "Added":
            explaination += ",extra filter are needed to be applied on tables during "
            for i in set(diffInQueryPlan["diffInScanNode"]):
                explaination += i
                if size1 == 1:
                    explaination += ". "
                else:
                    explaination += " or "
                    size1 -= 1
        elif status == "Removed":
            explaination += ",lesser to none filter are required for scans. "
        elif status == "Modified":
            explaination += ",extra filtering is not needed. "
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
            explaination += "NEVER SEEN"
    for i in set(diffInQueryPlan["diffInGroupNode"]):
        if i == "Aggregate":
            explaination += "Grouping techniqes such as aggregation are used. "
        else:
            explaination += "NEVER SEEN"
    if size > 0:
        explaination += "Such joined operaion have been used: "
    for i in set(diffInQueryPlan["diffInJoinNode"]):
        explaination += i
        if size == 1:
            explaination += ". "
        else:
            explaination += " and "
            size -= 1
    explaination = explaination.replace("(", "").replace(")", "").replace("'", "")
    print(explaination)
    
def queryplann(output):
    global toggle
    if output[2] == None:
        print("No changes to query plan")
    else:
        Query.append("Changes: ")
        print_nodes(output[2])
        toggle = 1
        print(" ")
        Query.append("Original: ")
        print_nodes(output[1])

    # Split the original list into two separate lists
    for i, item in enumerate(Query):
        if item == "Changes: ":
            changes = Query[i+1:]
        elif item == "Original: ":
            original = Query[i+1:]
            break
    original_idx = changes.index('Original: ')
    changes = changes[:original_idx]
    for i in diffInQueryPlan:
        print(diffInQueryPlan[i])
    #print("changes list are: " + str(changes))
    #print("original list are: " + str(original))
    
def print_nodes(node):
    print(node.node)
    Query.append(node.node)
    if type(node) == QueryPlanJoinNode and toggle == 0:
            #print(node.JoinCond)
        diffInQueryPlan["diffInJoinNode"].append(node.node)
    if type(node) == QueryPlanScanNode and toggle == 0:
        diffInQueryPlan["diffInScanNode"].append(node.node)
    if type(node) == QueryPlanSortNode and toggle == 0:
        diffInQueryPlan["diffInSortNode"].append(node.node)
    if type(node) == QueryPlanGroupNode and toggle == 0:
        diffInQueryPlan["diffInGroupNode"].append(node.node)
    if type(node) == QueryPlanNode and toggle == 0:
        diffInQueryPlan["diffInPlan"].append(node.node)
    if node.left:
        print_nodes(node.left)
    if node.right:
        print_nodes(node.right)

explain()