from project import QueryPlan, QueryPlanJoinNode, query
import re

diffInQuery = {
    'Removed': ["AND r_name = 'ASIA'", 'AND s_acctbal > 20'],
    'Modified': [["  AND o_orderdate < '1995-01-01'", "  AND o_orderdate < '1996-01-01'"], ['  AND s_acctbal > 20', '  AND s_acctbal > 10']],
    'Added': ["AND customer.name LIKE 'cheng'", 'OR s_acctbal > 20']
}

diffInPlan = []
#def explain(diffInQuery, diffInPlan):
def explain():
    explaination = ""
    filter_exp = r"(?i)(?:AND|NOT|OR|WHERE)\s+(.*)"

    count = 0
    for status, values in diffInQuery.items():
        if count == 1:
            explaination += "and with "
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

    explaination = explaination.replace("(", "").replace(")", "").replace("'", "")
    print(explaination)
    #SELECT n_name, o_year, sum(amount) AS sum_profit FROM(SELECT n_name, DATE_PART('YEAR',o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount FROM part, supplier, lineitem, partsupp, orders, nation WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name like '%green%' AND s_acctbal > 10 AND ps_supplycost > 100 ) AS profit GROUP BY n_name, o_year ORDER BY n_name, o_year desc

    query_1 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    query_2 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND c.c_name LIKE '%cheng'"

    q_plan_1 = query(query_1)
    q_plan_2 = query(query_2)

    q_plan_1_nodes = QueryPlan(q_plan_1["Plan"])
    q_plan_2_nodes = QueryPlan(q_plan_2["Plan"])
    
    output = q_plan_1_nodes.IsEqual(q_plan_2_nodes)
    print_nodes(output[1])
    print(diffInPlan)
    
def print_nodes(node):
    print(node.node)
    if type(node) == QueryPlanJoinNode:
        #print(node.JoinCond)
        diffInPlan.append("Changes: " + node.node + " between " + node.JoinCond + " ")
    if node.left:
        print_nodes(node.left)
    if node.right:
        print_nodes(node.right)

explain()



    #diffInPlan = Selection/Join/Projection

    #Types of Join:
        #Types of Nested Loop(NL) Join:
            # Tuple Based NL Join
            # Simple NL Join
            # Block-based NL Join
        #Sort-Based Algorithms:
            #Sort Merge Join
            #Refine Merge Join
        #Hash-Based Algorithms:
            #Grace Hash Join
            #Hybrid Hash Join
        #Index-Based Algorithms:
            #Index-Based Join:
                #Clustered
                #Uncluctered