from project import *
import re

Query = []
diffInQueryPlan = {
    "diffInJoinNode": [],
    "diffInScanNode": [],
    "diffInGroupNode": [],
    "diffInSortNode": [],
    "diffInPlan": [],
}
queries_subset = []
filter_exp = r"(?i)(?:AND|NOT|OR|WHERE)\s+(.*)"


def explain(diffInQuery, diffInPlan):
    # Remove Empty List
    diffInQuery = {i: j for i, j in diffInQuery.items() if j != []}
    # for i in diffInQueryPlan:
    #     diffInQueryPlan[i].clear
    print("diffInQuery:", diffInQuery)
    print("diffInPlan:", diffInPlan)

    print_nodes(diffInPlan)
    gettingAdd(diffInQuery)
    table1 = []
    table1.clear()
    table1 = joiningsplit()

    explaination = ""

    print("diffInQueryPlan:", diffInQueryPlan)
    print("queries_subset:", queries_subset)
    for i in diffInQueryPlan:
        print(diffInQueryPlan[i])

    count = 0
    toskip = 0
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
                match_from = re.search(filter_exp, " ".join(value[0].split()))
                match_to = re.search(filter_exp, " ".join(value[1].split()))

                if match_to and match_from:
                    if count1 == 0 or toskip == 1:
                        explaination += "join condition that changes from "
                        toskip = 0
                    if count1 % 2 == 0 and count1 != 0:
                        explaination += "and "
                        count1 = 0
                    explaination += f'"{match_from.group(1)}" to "{match_to.group(1)}" '
                    count1 += 2
                else:
                    if count1 == 2:
                        explaination += "and "
                    explaination += "condition outside of the where clause, "
                    toskip = 1
            else:
                match = re.search(filter_exp, " ".join(value.split()))
                if match:
                    if count1 == 0:
                        explaination += "join condition such as "
                    elif count1 % 2 == 1:
                        explaination += "and "
                        count1 = 0
                    explaination += f'"{match.group(1)}" '
                    count1 += 1
                else:
                    explaination += "some condition outside of the where clause, "
                    toskip = 1
        count = 1
        if status == "Added":
            explaination = explaination.rstrip()
            if toskip == 0:
                explaination += ", extra filter are needed during "
            for i in queries_subset:
                explaination += i
                if size1 == 1:
                    explaination += ". \n"
                else:
                    explaination += " and "
                    size1 -= 1
        elif status == "Removed":
            explaination = explaination.rstrip()
            if toskip == 0:
                explaination += ", lesser to none filter are required for scans. "
        elif status == "Modified":
            explaination = explaination.rstrip()
            if toskip == 0:
                explaination += ", no extra filtering is needed. "
    for i in set(diffInQueryPlan["diffInPlan"]):
        if i == "Gather Merge":
            explaination += "\nThe query plan uses a Gather Merge that combines the output of its child nodes, which are executed by parallel workers. "
        elif i == "Gather":
            explaination += "\nThe query plan uses a Gather operation to distribute the scanning of the table among two parallel workers. "
        elif i == "Hash":
            explaination += "\nHashing have been used. "
        elif i == "Sort":
            explaination += "\nSorting have been used. "
        elif i == "Materialize":
            explaination += "\nThe query plan uses Materialization to the process of creating an intermediate result set. This maybe due to the size of the result set being too large to fit into memory or when multiple operations are performed on the same data. "
        elif i == "Memoize":
            explaination += "\nThe query plan uses Memoize to improve the performance of recursive queries. "
        else:
            explaination += "\nThe query plan uses " + i + ". "
    for i in set(diffInQueryPlan["diffInGroupNode"]):
            explaination += "\nGrouping techniqes such as " + i + " are used. "
    if size > 0:
        explaination += "\nSuch joined operation have been used: "
    for i in diffInQueryPlan["diffInJoinNode"]:
        for x in range(0,len(table1),2):
            if x == 0:
                explaination += "\nTable " + table1[x] + " and table " + table1[x+1] + " is joined using " + i.split("-")[0] + "with join condition" + i.split("-")[1] + ". "
    for i in set(diffInQueryPlan["diffInSortNode"]):
        explaination += i + " ."

    return explaination


def get_table_name(text):
    # match the last word before the dot
    match = re.search(r"\b(\w+)\.", text)
    if match:
        return match.group(1)
    else:
        return None
    
def joiningsplit() -> list:
    table = []
    table.clear()
    for i in diffInQueryPlan["diffInJoinNode"]:
        temp = i.split("-")[1]
        table.append(get_table_name(temp.split("=")[0].strip()))
        table.append(get_table_name(temp.split("=")[1].strip()))
    return(table)



def format_string(text):
    # replace double colon followed by word with empty string
    text = text.replace("(", "").replace(")", "").replace("~~", "LIKE")
    return re.sub(r"::\w+", "", text)


def print_nodes(nodes):
    for i in diffInQueryPlan:
        diffInQueryPlan[i].clear()
    for node in nodes:
        Query.append(node.node)
        if type(node) == QueryPlanJoinNode and node.JoinCond != "":
            diffInQueryPlan["diffInJoinNode"].append(node.node + " - " + node.JoinCond)
        if type(node) == QueryPlanScanNode:
            if node.Filter != "":
                filter = format_string(node.Filter)
                if node.Alias != node.RelationName:
                    filter = f"{node.Alias}.{filter}"
                diffInQueryPlan["diffInScanNode"].append(
                    filter + "- " + node.node + " on: " + node.RelationName
                )
        if type(node) == QueryPlanSortNode:
            diffInQueryPlan["diffInSortNode"].append(node.node)
        if type(node) == QueryPlanGroupNode:
            diffInQueryPlan["diffInGroupNode"].append(node.node)
        if type(node) == QueryPlanNode:
            diffInQueryPlan["diffInPlan"].append(node.node)


def gettingAdd(diffInQuery):
    global queries_subset
    matching_indices = []
    for i, s in enumerate(diffInQueryPlan["diffInScanNode"]):
        if diffInQuery.get("Added"):
            for q in diffInQuery["Added"]:
                conditions = q.split('AND')
                extracted_conditions = [condition.strip() for condition in conditions]
                extracted_conditions = list(filter(None, extracted_conditions))
                print("s:", s.replace("'", ""))
                print("q:", extracted_conditions[0].replace("'", ""))
                for o in range(len(extracted_conditions)):
                    if extracted_conditions[o].replace("'", "").lower() in s.replace("'", "").lower():
                        matching_indices.append(i)

    queries_subset = [diffInQueryPlan["diffInScanNode"][i] for i in matching_indices]
    queries_subset = [s.split("-")[1].strip() for s in queries_subset]
