from project import QueryPlan, QueryPlanJoinNode, query
import re

diffInQuery = {
    'Removed': ["AND r_name = 'ASIA'", 'AND s_acctbal > 20'],
    'Modified': [["  AND o_orderdate < '1995-01-01'"], ["  AND o_orderdate < '1996-01-01'"], ['  AND s_acctbal > 20'], ['  AND s_acctbal > 10']],
    'Added': ["AND customer.name LIKE 'cheng'"]
}

#def explain(diffInQuery, diffInPlan):
def explain():
    diff = []
    for key, value in diffInQuery.items():
        diff.append(f"{key}: {str(value)}")

    diff = [s.replace("'", "\'") for s in diff]

    for i in range(len(diff)):
        diff[i] = diff[i].replace("[[", "[").replace("]]", "]")
        if "[" in diff[i]:
            diff[i] = diff[i][:-1] + "', " + diff[i][-1]
        else:
            diff[i] = diff[i][:-1] + "']" + diff[i][-1]
    explaination = ""
    
    # Define removed/modified/added
    status_regex = re.compile(r'(Removed|Modified|Added):\s*')
    diff_regex = re.compile(r':\s*(.*)')
    # Define regular expressions for each component type
    comparison_regex = re.compile(r"([\w\.]+)\s*(=|!=|<|>|<=|>=|<>|LIKE)\s*('[\w-]+'|[\w\.]+)")

    count = 0
    for item in diff:
        if count == 1:
            explaination += "and with "
        else:
            explaination += "With "
        status_match = status_regex.match(item)
        if status_match:
            status = status_match.group(1)
            if status == 'Added':
                explaination += "an addition of "
            elif status == 'Removed':
                explaination += "the removal of "
            elif status == 'Modified':
                explaination += "modification of "
            diff_match = diff_regex.search(item)
            count = 1
            if diff_match:
                diff = diff_match.group(1)
                #print(f"Status: {status}, Diff: {diff}")
                input_str = diff
        
        # Initialize lists to store each type of expression
        comparisons = []

        comparison_matches = comparison_regex.findall(input_str)
        for match in comparison_matches:
            comparisons.append(match)

        # Print the resulting lists
        print("Comparisons:", comparisons)
        count1 = 0
        if status == 'Modified':
            explaination += "join condition that changes from "
            for i in comparisons:
                if count1%2 == 0 and count1 != 0:
                    explaination += "and "
                    count1 = 0
                for j in i:
                    explaination += (str(j)) + " "
                if count1 == 0:
                    explaination += "to "
                count1 += 1
        else:
            explaination += "join condition such as "
            for i in comparisons:
                if count1%2 == 1:
                    explaination += "and "
                    count1 = 0
                explaination += " ".join(str(j) for j in i) + " "
                count1 += 1


    query_1 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey"
    query_2 = "SELECT * FROM customer C, orders O WHERE C.c_custkey = O.o_custkey AND c.c_name LIKE '%cheng'"

    q_plan_1 = query(query_1)
    q_plan_2 = query(query_2)

    q_plan_1_nodes = QueryPlan(q_plan_1["Plan"])
    q_plan_2_nodes = QueryPlan(q_plan_2["Plan"])

    #print(q_plan_1_nodes.root.JoinCond)
    #print(q_plan_1_nodes.root.node)
    #print_nodes(q_plan_1_nodes.root)
    #build_tree_diff(q_plan_1_nodes.root)

    #diffInPlan = Selection/Join/Projection
    # queryTwo = QueryPlan.root.getJoinConditions()

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

    explaination += ",the query plan uses sort-merge join instead. \nBecause,"
    explaination = explaination.replace("(", "").replace(")", "").replace("'", "")
    print(explaination)

def print_nodes(node):
    if node is None:
        return
    print(node)
    print_nodes(node.left)
    print_nodes(node.right)

def build_tree_diff(node, parent=None):
    current_node = node, parent=parent
    print(current_node)
    if node.left:
        build_tree_diff(node.left, parent=current_node)
    if node.right:
        build_tree_diff(node.right, parent=current_node)

    return current_node  

explain()
